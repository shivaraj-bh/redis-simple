{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Redis.Connection where

import Control.Exception (Exception, throwIO)
import qualified Data.ByteString as BS
import qualified Data.Pool as Pool
import Database.Redis.Protocol (Reply (MultiBulk, Integer, Bulk), reply, renderRequest)
import qualified Network.Socket as Socket
import qualified Scanner
import qualified System.IO as IO
import Control.Concurrent (MVar, newMVar, readMVar)
import qualified Data.ByteString.Char8 as Char8
import Database.Redis.HashSlot (keyToSlot)
import Control.Monad (filterM)

data ClusteredNode conn = Node
  { nodeConns :: Pool.Pool conn
    -- ^ Pool of connections to this node
  , nodeInfo :: MVar (Int, Int, Socket.HostName, Socket.PortNumber)
    -- ^ (Slot start, Slot end, Hostname, Port)
  }

data MasterNode conn = MasterNode (ClusteredNode conn) [SlaveNode conn]

newtype SlaveNode conn = SlaveNode (ClusteredNode conn)

data ConnectionPool conn
  = Clustered [MasterNode conn]
  | NonClustered (Pool.Pool conn)

newtype Connection = Connection (ConnectionPool IO.Handle)

data ConnectionLostException = ConnectionLost deriving (Show)

instance Exception ConnectionLostException

data ConnectInfo = ConnInfo
  {
    connectHost :: Socket.HostName
  , connectPort :: Socket.PortNumber
  , connectMaxConnections :: Int
  , connectMaxIdleTime :: Double
  , cluster :: Bool
  }

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
    destroyConnection conn
    parseClusterSlots (head clusterSlots)
  else
    Connection . NonClustered <$> Pool.newPool (poolConfig connectInfo)

parseClusterSlots :: Reply -> IO Connection
parseClusterSlots (MultiBulk (Just bulkData)) = do
  nodes <- mapM parseNodes bulkData
  return $ Connection (Clustered nodes)
parseClusterSlots _ = error "redis-simple: received invalid reply from redis server"

parseNodes :: Reply -> IO (MasterNode IO.Handle)
parseNodes (MultiBulk (Just ((Integer startSlot):(Integer endSlot):masterData:replicas)))= do
  master <- createNode (fromIntegral startSlot) (fromIntegral endSlot) masterData
  -- Use dummy values for start and end slot for slaves 
  slaves <- mapM (createNode 0 0) replicas
  return $ MasterNode master (map SlaveNode slaves)
parseNodes _ = error "redis-simple: received invalid reply from redis server"

createNode :: Int -> Int -> Reply -> IO (ClusteredNode IO.Handle)
createNode startSlot endSlot (MultiBulk (Just ((Bulk (Just host)):(Integer port):_:_))) = do
  pool <- Pool.newPool (poolConfig defaultConnectInfo { connectHost = Char8.unpack host, connectPort = fromIntegral port })
  nodeInfo <- newMVar (startSlot, endSlot, Char8.unpack host, fromIntegral port :: Socket.PortNumber)
  return $ Node pool nodeInfo
createNode _ _ _ = error "redis-simple: received invalid reply from redis server"

getMasterNode :: MasterNode IO.Handle -> ClusteredNode IO.Handle
getMasterNode (MasterNode master _) = master
isInRange :: Int -> MasterNode IO.Handle -> IO Bool
isInRange n master = do
  (startSlot, endSlot, _, _) <- readMVar $ nodeInfo (getMasterNode master)
  return $ n >= startSlot && n <= endSlot
runRedis :: Connection -> [[BS.ByteString]] -> IO [Reply]
runRedis conn commands =
  case conn of
    Connection (NonClustered pool) ->
      Pool.withResource pool $ \handle -> sendRecv handle commands
    Connection (Clustered masters) -> do
      -- FIXME: only meant to work for set commands atm
      let key = head commands !! 2
          hashslot = keyToSlot key
      master <- filterM (isInRange (fromEnum hashslot)) masters
      let pool = nodeConns $ getMasterNode (head master)
      Pool.withResource pool $ \handle -> sendRecv handle commands

