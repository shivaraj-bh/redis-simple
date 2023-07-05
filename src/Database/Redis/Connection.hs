{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Redis.Connection where

import qualified Data.ByteString as BS
import qualified Network.Socket as Socket
import qualified Data.Pool as Pool
import qualified System.IO as IO
import Data.List (unfoldr)

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

splitOnCRLF :: BS.ByteString -> [BS.ByteString]
splitOnCRLF = unfoldr f
  where
    f bs' = case BS.breakSubstring "\r\n" bs' of
      (x, y) | BS.null y -> if BS.null x then Nothing else Just (x, BS.empty)
             | otherwise -> Just (x, BS.drop 2 y)

sendCommands :: IO.Handle -> [ BS.ByteString ] -> IO [ BS.ByteString ]
sendCommands handle commands =
    -- Send commands to Redis
    BS.hPut handle (BS.concat commands)
    -- Read the response and split
    >> splitOnCRLF <$> BS.hGetSome handle 4096

createConnection ::[Socket.AddrInfo] -> IO IO.Handle
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

runRedis :: Pool.Pool IO.Handle -> [ BS.ByteString ] -> IO [ BS.ByteString ]
runRedis pool commands = Pool.withResource pool $ \handle -> sendCommands handle commands

