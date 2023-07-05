{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Redis.Connection where

import Control.Exception (Exception, throwIO)
import Control.Monad (foldM)
import qualified Data.ByteString as BS
import qualified Data.Pool as Pool
import Database.Redis.Protocol (Reply, reply)
import qualified Network.Socket as Socket
import qualified Scanner
import qualified System.IO as IO

sendRecv :: IO.Handle -> [BS.ByteString] -> IO [Reply]
sendRecv handle commands = do
  -- Send commands to Redis
  BS.hPut handle (BS.concat commands)
  -- Read the response and parse
  resp <- BS.hGetSome handle 4096
  parseReplies resp

data ConnectionLostException = ConnectionLost deriving (Show)

instance Exception ConnectionLostException

errConnClosed :: IO a
errConnClosed = throwIO ConnectionLost

data ConnectInfo = ConnInfo
  {
    connectHost :: Socket.HostName
  , connectPort :: Socket.PortNumber
  , connectMaxConnections :: Int
  , connectMaxIdleTime :: Double
  }
defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnInfo
  {
    connectHost = "localhost"
  , connectPort = 6379
  , connectMaxConnections = 50
  , connectMaxIdleTime = 30
  }

parseReplies :: BS.ByteString -> IO [Reply]
parseReplies input = do
  (_, replies) <- foldM scanStep (input, []) [1 ..]
  return (reverse replies)
  where
    scanStep (acc, replies) _ = do
      let scanResult = Scanner.scan reply acc
      case scanResult of
        Scanner.Fail {} -> errConnClosed
        Scanner.More {} -> return (acc, replies)
        Scanner.Done rest' r -> return (rest', r : replies)

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
connect :: ConnectInfo -> IO (Pool.Pool IO.Handle)
connect = Pool.newPool . poolConfig

runRedis :: Pool.Pool IO.Handle -> [BS.ByteString] -> IO [Reply]
runRedis pool commands = Pool.withResource pool $ \handle -> sendRecv handle commands

