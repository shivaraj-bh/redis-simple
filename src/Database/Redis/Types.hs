-- Based on https://github.com/informatikr/hedis/blob/master/src/Database/Redis/Types.hs
{-# LANGUAGE CPP, FlexibleInstances #-}

module Database.Redis.Types where

import qualified Data.Pool as Pool
import Control.Concurrent (MVar)
import qualified Network.Socket as Socket
import qualified System.IO as IO
import Control.Exception (Exception)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HM
import Data.Data (Typeable)

data ClusteredNode conn = Node
  { nodeConns :: Pool.Pool conn
    -- ^ Pool of connections to this node
  , nodeInfo :: MVar (Socket.HostName, Socket.PortNumber)
    -- ^ (Hostname, Port)
  }

data MasterNode conn = MasterNode (MVar (Int, Int)) (ClusteredNode conn) [SlaveNode conn]
--                               ^ Slot (start, end)
newtype SlaveNode conn = SlaveNode (ClusteredNode conn)

data ConnectionPool conn
  = Clustered [MasterNode conn] InfoMap
  | NonClustered (Pool.Pool conn)

newtype Connection = Connection (ConnectionPool IO.Handle)

data ConnectionLostException = ConnectionLost deriving (Show)
newtype UnsupportedClusterCommandException = UnsupportedClusterCommandException [BS.ByteString] deriving (Show, Typeable)
newtype CrossSlotException = CrossSlotException [[BS.ByteString]] deriving (Show, Typeable)


data ConnectInfo = ConnInfo
  {
    connectHost :: Socket.HostName
  , connectPort :: Socket.PortNumber
  , connectMaxConnections :: Int
  , connectMaxIdleTime :: Double
  , cluster :: Bool
  }

data Flag
    = Write
    | ReadOnly
    | DenyOOM
    | Admin
    | PubSub
    | NoScript
    | Random
    | SortForScript
    | Loading
    | Stale
    | SkipMonitor
    | Asking
    | Fast
    | MovableKeys
    | Other BS.ByteString deriving (Show, Eq)


data AritySpec = Required Integer | MinimumRequired Integer deriving (Show)

data LastKeyPositionSpec = LastKeyPosition Integer | UnlimitedKeys Integer deriving (Show)

newtype InfoMap = InfoMap (HM.HashMap String CommandInfo)

-- Represents the result of the COMMAND command, which returns information
-- about the position of keys in a request
data CommandInfo = CommandInfo
    { name :: BS.ByteString
    , arity :: AritySpec
    , flags :: [Flag]
    , firstKeyPosition :: Integer
    , lastKeyPosition :: LastKeyPositionSpec
    , stepCount :: Integer
    } deriving (Show)
------------------------------------------------------------------------------
-- Exception instances

instance Exception ConnectionLostException
instance Exception UnsupportedClusterCommandException
instance Exception CrossSlotException





