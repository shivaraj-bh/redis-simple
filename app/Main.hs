{-# LANGUAGE GADTs, OverloadedStrings #-}
import Control.Concurrent.Async ( async )
import Control.Concurrent.STM
    ( atomically,
      newEmptyTMVarIO,
      putTMVar,
      takeTMVar,
      newTQueueIO,
      readTQueue,
      writeTQueue,
      TMVar,
      TQueue )
import Control.Monad ( forever, replicateM, replicateM_ )
import Data.Foldable (traverse_)
import Database.Redis (connect, runRedis)
import Data.Pool (Pool)
import System.IO (Handle)
import Data.ByteString (ByteString)
import Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar, MVar)
import Data.Time (diffUTCTime, getCurrentTime)
import System.CPUTime (getCPUTime)

type RedisQuery = ByteString

data QueryBuffer where
  QueryBuffer ::
    { queryQueue :: TQueue ( RedisQuery, TMVar ByteString ) } -> QueryBuffer

newQueryBuffer :: IO QueryBuffer
newQueryBuffer = QueryBuffer <$> newTQueueIO

bufferRedisQuery :: QueryBuffer -> RedisQuery -> IO (TMVar ByteString)
bufferRedisQuery (QueryBuffer queue) query = do
  resultVar <- newEmptyTMVarIO
  atomically $ writeTQueue queue (query, resultVar)
  return resultVar

runConsumer :: Pool Handle -> QueryBuffer -> Int -> IO ()
runConsumer conn (QueryBuffer queue) batchSize =
  let consumeQueryBatch :: [(RedisQuery, TMVar ByteString)] -> IO ()
      consumeQueryBatch queryBatch = do
        results <- runRedis conn (map fst queryBatch)
        traverse_ (uncurry handleResult) (zip queryBatch results)

      handleResult :: (RedisQuery, TMVar ByteString) -> ByteString -> IO ()
      handleResult (_, resultVar) reply =
        atomically $ putTMVar resultVar reply

  in forever $ do
    queryBatch <- atomically $ replicateM batchSize (readTQueue queue)
    consumeQueryBatch queryBatch

data Config = Config
  { clients :: Int
  , requests :: Int
  , bufferSize :: Int
  }

main :: IO ()
main = do
  -- connection pool
  conn <- connect
  -- MVar to synchronize the client threads
  done <- newEmptyMVar
  -- Start the consumer thread
  queryBuffer <- newQueryBuffer
  let consumerAction = runConsumer conn queryBuffer (bufferSize myConfig)
  _ <- async consumerAction

  -- Run the benchmarks
  deltaT "Wall-clock time (with batching)" $ 
    deltaCPUT "CPU time (with batching)" $ 
      bufferTest done queryBuffer
  deltaT "Wall-clock time (without batching)" $ 
    deltaCPUT "CPU time (without batching)" $ 
      withoutBufferTest done conn
  where
  myConfig :: Config
  myConfig = Config
    { clients = 500
    , requests = 200
    , bufferSize = 10
    }
  deltaT :: String -> IO () -> IO ()
  deltaT msg action = do
    start <- getCurrentTime
    action
    end <- getCurrentTime
    let diff = realToFrac (diffUTCTime end start) * 1000 :: Double
    putStrLn $ msg ++ ": " ++ show diff ++ " ms"
  deltaCPUT :: String -> IO () -> IO ()
  deltaCPUT msg action = do
    start <- getCPUTime
    action
    end <- getCPUTime
    let diff = fromIntegral (end - start) / (10^(9 :: Int)) :: Double
    putStrLn $ msg ++ ": " ++ show diff ++ " ms"
  bufferTest :: MVar () -> QueryBuffer -> IO ()
  bufferTest done queryBuffer = do
    replicateM_ (clients myConfig) $ forkIO $ do
      resultVars <- replicateM (requests myConfig) $ bufferRedisQuery queryBuffer "SET key val\r\n"
      -- let's not worry about printing the results for now
      _ <- atomically $ mapM takeTMVar resultVars
      putMVar done ()
    replicateM_ (clients myConfig) $ takeMVar done
  withoutBufferTest :: MVar () -> Pool Handle -> IO ()
  withoutBufferTest done conn = do
    replicateM_ (clients myConfig) $ forkIO $ do
      replicateM_ (requests myConfig) $ runRedis conn [ "SET key val\r\n" ]
      putMVar done ()
    replicateM_ (clients myConfig) $ takeMVar done
