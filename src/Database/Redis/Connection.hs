{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Redis.Connection where

import Control.Exception (throwIO)
import qualified Data.ByteString as BS
import qualified Data.Pool as Pool
import Database.Redis.Protocol (Reply (MultiBulk, Integer, Bulk, SingleLine), reply, renderRequest)
import qualified Network.Socket as Socket
import qualified Scanner
import qualified System.IO as IO
import Control.Concurrent (newMVar, readMVar, MVar)
import qualified Data.ByteString.Char8 as Char8
import Database.Redis.HashSlot (hashSlotForKeys)
import Control.Monad (filterM)
import Database.Redis.Types
import qualified Data.HashMap.Strict as HM
import Data.Char (toLower)


sendRecv :: IO.Handle -> [[BS.ByteString]] -> IO [Reply]
sendRecv handle commands = do
  -- Send commands to Redis
  BS.hPut handle (BS.concat (map renderRequest commands))
  -- Read the response and parse
  resp <- BS.hGetSome handle 4096
  parseReplies resp


errConnClosed :: IO a
errConnClosed = throwIO ConnectionLost

defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnInfo
  {
    connectHost = "localhost"
  , connectPort = 6379
  , connectMaxConnections = 50
  , connectMaxIdleTime = 30
  , cluster = False
  }

parseReplies :: BS.ByteString -> IO [Reply]
parseReplies = go
  where
    go "" = return []
    go rest = do
      (r, rest') <- do
        let scanResult = Scanner.scan reply rest
        case scanResult of
          Scanner.Fail {} -> errConnClosed
          Scanner.More {} -> error "redis-simple: received partial reply from redis server"
          Scanner.Done rest' r -> return (r, rest')
      rs <- go rest'
      return (r : rs)

createConnection :: [Socket.AddrInfo] -> IO IO.Handle
createConnection addrInfos = do
  s <- Socket.socket (Socket.addrFamily (head addrInfos)) Socket.Stream Socket.defaultProtocol
  Socket.connect s (Socket.addrAddress (head addrInfos))
  Socket.setSocketOption s Socket.KeepAlive 1
  Socket.socketToHandle s IO.ReadWriteMode

destroyConnection :: IO.Handle -> IO ()
destroyConnection = IO.hClose

createResource :: Socket.HostName -> Socket.PortNumber -> IO IO.Handle
createResource hostname port = do
  addrInfo <-  Socket.getAddrInfo (Just hints) (Just hostname) (Just $ show port)
  createConnection addrInfo
  where
    hints = Socket.defaultHints
      { Socket.addrSocketType = Socket.Stream }

poolConfig :: ConnectInfo -> Pool.PoolConfig IO.Handle
poolConfig ConnInfo{..} = Pool.defaultPoolConfig (createResource connectHost connectPort) destroyConnection connectMaxIdleTime connectMaxConnections

-- Abstract out pool creation 
connect :: ConnectInfo -> IO Connection
connect connectInfo =
  if cluster connectInfo then do
    conn <- createResource (connectHost connectInfo) (connectPort connectInfo)
    clusterSlots <- sendRecv conn [["CLUSTER", "SLOTS"]]
    commandInfo <- sendRecv conn [["COMMAND"]]
    destroyConnection conn
    connectCluster (head clusterSlots) (head commandInfo)
  else
    Connection . NonClustered <$> Pool.newPool (poolConfig connectInfo)

parseCommandInfos :: Reply -> IO [CommandInfo]
parseCommandInfos (MultiBulk (Just bulkData)) = do
  let parsed = mapM parseCommandInfo bulkData
  either (error . ("redis-simple: invalid response from redis" ++) . show) return parsed
parseCommandInfos bad = error $ "redis-simple: received invalid reply from redis server: " ++ show bad

parseCommandInfo :: Reply -> Either Reply CommandInfo
parseCommandInfo (MultiBulk (Just [ Bulk (Just commandName), Integer aritySpec, MultiBulk (Just replyFlags), Integer firstKeyPos, Integer lastKeyPos, Integer replyStepCount])) = do
  parsedFlags <- mapM parseFlag replyFlags
  lastKey <- parseLastKeyPos
  return $ CommandInfo
      { name = commandName
      , arity = parseArity aritySpec
      , flags = parsedFlags
      , firstKeyPosition = firstKeyPos
      , lastKeyPosition = lastKey
      , stepCount = replyStepCount
      } 
  where
    parseArity int = case int of
        i | i >= 0 -> Required i
        i -> MinimumRequired $ abs i
    parseFlag :: Reply -> Either Reply Flag
    parseFlag (SingleLine flag) = return $ case flag of
        "write" -> Write
        "readonly" -> ReadOnly
        "denyoom" -> DenyOOM
        "admin" -> Admin
        "pubsub" -> PubSub
        "noscript" -> NoScript
        "random" -> Random
        "sort_for_script" -> SortForScript
        "loading" -> Loading
        "stale" -> Stale
        "skip_monitor" -> SkipMonitor
        "asking" -> Asking
        "fast" -> Fast
        "movablekeys" -> MovableKeys
        other -> Other other
    parseFlag bad = Left bad
    parseLastKeyPos :: Either Reply LastKeyPositionSpec
    parseLastKeyPos = return $ case lastKeyPos of
        i | i < 0 -> UnlimitedKeys (-i - 1)
        i -> LastKeyPosition i
-- since redis 6.0 (ignore the last reply, ACL categories)
parseCommandInfo (MultiBulk (Just[ name@(Bulk (Just _)), arity@(Integer _), flags@(MultiBulk (Just _)), firstPos@(Integer _), lastPos@(Integer _), step@(Integer _), MultiBulk _ ])) = parseCommandInfo (MultiBulk (Just [name, arity, flags, firstPos, lastPos, step]))
-- since redis 6.0 (ignore the last replies, i.e ACL categories, Tips, Key Specifications, Sub Commands)
parseCommandInfo (MultiBulk (Just[ name@(Bulk (Just _)), arity@(Integer _), flags@(MultiBulk (Just _)), firstPos@(Integer _), lastPos@(Integer _), step@(Integer _), MultiBulk _, MultiBulk _, MultiBulk _, MultiBulk _])) = parseCommandInfo (MultiBulk (Just [name, arity, flags, firstPos, lastPos, step]))
parseCommandInfo bad = error $ "redis-simple: received invalid reply from redis server: " ++ show bad

newInfoMap :: [CommandInfo] -> InfoMap
newInfoMap = InfoMap . HM.fromList . map (\c -> (Char8.unpack $ name c, c))

connectCluster :: Reply -> Reply -> IO Connection
connectCluster clusterSlots commandInfos = do
  masters <- parseClusterSlots clusterSlots
  commands <- parseCommandInfos commandInfos 
  return $ Connection (Clustered masters (newInfoMap commands))

parseClusterSlots :: Reply -> IO [MasterNode IO.Handle]
parseClusterSlots (MultiBulk (Just bulkData)) = mapM parseNodes bulkData
parseClusterSlots bad = error $ "redis-simple: received invalid reply from redis server" ++ show bad

parseNodes :: Reply -> IO (MasterNode IO.Handle)
parseNodes (MultiBulk (Just ((Integer startSlot):(Integer endSlot):masterData:replicas)))= do
  master <- createNode masterData
  -- Use dummy values for start and end slot for slaves 
  slaves <- mapM createNode replicas
  slotsMVar <- newMVar (fromIntegral startSlot, fromIntegral endSlot)
  return $ MasterNode slotsMVar master (map SlaveNode slaves)
parseNodes bad = error $ "redis-simple: received invalid reply from redis server" ++ show bad

createNode :: Reply -> IO (ClusteredNode IO.Handle)
createNode (MultiBulk (Just ((Bulk (Just host)):(Integer port):_:_))) = do
  pool <- Pool.newPool (poolConfig defaultConnectInfo { connectHost = Char8.unpack host, connectPort = fromIntegral port })
  nodeInfo <- newMVar (Char8.unpack host, fromIntegral port :: Socket.PortNumber)
  return $ Node pool nodeInfo
createNode bad = error $ "redis-simple: received invalid reply from redis server" ++ show bad

getSlotsMVar :: MasterNode IO.Handle -> MVar (Int, Int)
getSlotsMVar (MasterNode slot _ _) = slot

getMasterNode :: MasterNode IO.Handle -> ClusteredNode IO.Handle
getMasterNode (MasterNode _ master _) = master

isInRange :: Int -> MasterNode IO.Handle -> IO Bool
isInRange n master = do
  (startSlot, endSlot) <- readMVar $ getSlotsMVar master
  return $ n >= startSlot && n <= endSlot
runRedis :: Connection -> [[BS.ByteString]] -> IO [Reply]
runRedis conn commands =
  case conn of
    Connection (NonClustered pool) ->
      Pool.withResource pool $ \handle -> sendRecv handle commands
    Connection (Clustered masters infoMap) -> do
      -- FIXME: only meant to work for set commands atm
      keys <- mconcat <$> mapM (requestKeys infoMap) commands
      hashSlot <- hashSlotForKeys (CrossSlotException commands) keys
      master <- filterM (isInRange (fromEnum hashSlot)) masters
      let pool = nodeConns $ getMasterNode (head master)
      Pool.withResource pool $ \handle -> sendRecv handle commands


requestKeys :: InfoMap -> [BS.ByteString] -> IO [BS.ByteString]
requestKeys infoMap request =
    case keysForRequest infoMap request of
        Nothing -> throwIO $ UnsupportedClusterCommandException request
        Just k -> return k
keysForRequest :: InfoMap -> [BS.ByteString] -> Maybe [BS.ByteString]
keysForRequest _ ["DEBUG", "OBJECT", key] =
    -- `COMMAND` output for `DEBUG` would let us believe it doesn't have any
    -- keys, but the `DEBUG OBJECT` subcommand does.
    Just [key]
keysForRequest _ ["QUIT"] =
    -- The `QUIT` command is not listed in the `COMMAND` output.
    Just []
keysForRequest (InfoMap infoMap) request@(command:_) = do
    info <- HM.lookup (map toLower $ Char8.unpack command) infoMap
    keysForRequest' info request
keysForRequest _ [] = Nothing

keysForRequest' :: CommandInfo -> [BS.ByteString] -> Maybe [BS.ByteString]
keysForRequest' info request
    | isMovable info =
        parseMovable request
    | stepCount info == 0 =
        Just []
    | otherwise = do
        let possibleKeys = case lastKeyPosition info of
                LastKeyPosition end -> take (fromEnum $ 1 + end - firstKeyPosition info) $ drop (fromEnum $ firstKeyPosition info) request
                UnlimitedKeys end ->
                    drop (fromEnum $ firstKeyPosition info) $
                       take (length request - fromEnum end) request
        return $ takeEvery (fromEnum $ stepCount info) possibleKeys

isMovable :: CommandInfo -> Bool
isMovable CommandInfo{..} = MovableKeys `elem` flags

parseMovable :: [BS.ByteString] -> Maybe [BS.ByteString]
parseMovable ("SORT":key:_) = Just [key]
parseMovable ("EVAL":_:rest) = readNumKeys rest
parseMovable ("EVALSHA":_:rest) = readNumKeys rest
parseMovable ("ZUNIONSTORE":_:rest) = readNumKeys rest
parseMovable ("ZINTERSTORE":_:rest) = readNumKeys rest
parseMovable ("XREAD":rest) = readXreadKeys rest
parseMovable ("XREADGROUP":"GROUP":_:_:rest) = readXreadgroupKeys rest
parseMovable _ = Nothing

readXreadKeys :: [BS.ByteString] -> Maybe [BS.ByteString]
readXreadKeys ("COUNT":_:rest) = readXreadKeys rest
readXreadKeys ("BLOCK":_:rest) = readXreadKeys rest
readXreadKeys ("STREAMS":rest) = Just $ take (length rest `div` 2) rest
readXreadKeys _ = Nothing

readXreadgroupKeys :: [BS.ByteString] -> Maybe [BS.ByteString]
readXreadgroupKeys ("COUNT":_:rest) = readXreadKeys rest
readXreadgroupKeys ("BLOCK":_:rest) = readXreadKeys rest
readXreadgroupKeys ("NOACK":rest) = readXreadKeys rest
readXreadgroupKeys ("STREAMS":rest) = Just $ take (length rest `div` 2) rest
readXreadgroupKeys _ = Nothing

readNumKeys :: [BS.ByteString] -> Maybe [BS.ByteString]
readNumKeys (rawNumKeys:rest) = do
    numKeys <- readMaybe (Char8.unpack rawNumKeys)
    return $ take numKeys rest
readNumKeys _ = Nothing
-- takeEvery 1 [1,2,3,4,5] ->[1,2,3,4,5]
-- takeEvery 2 [1,2,3,4,5] ->[1,3,5]
-- takeEvery 3 [1,2,3,4,5] ->[1,4]
takeEvery :: Int -> [a] -> [a]
takeEvery _ [] = []
takeEvery n (x:xs) = x : takeEvery n (drop (n-1) xs)

readMaybe :: Read a => String -> Maybe a
readMaybe s = case reads s of
                  [(val, "")] -> Just val
                  _           -> Nothing
