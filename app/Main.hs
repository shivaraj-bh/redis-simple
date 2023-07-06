{-# LANGUAGE GADTs, OverloadedStrings, BangPatterns #-}
-- queryQueue is unused and I am not comfortable seeing that warning.
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
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
import Control.Monad ( forever, replicateM, replicateM_, forM_, forM )
import Data.Foldable (traverse_)
import Database.Redis (connect, runRedis, defaultConnectInfo, Reply, Connection, ConnectInfo(..))
import Data.ByteString (ByteString)
import Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar, MVar)
import System.CPUTime (getCPUTime)
import Data.List.Split (chunksOf)
import System.Random (newStdGen, Random (randomRs))
import Data.ByteString.Char8 (pack)

type RedisQuery = [ ByteString ]

data QueryBuffer where
  QueryBuffer ::
    { queryQueue :: TQueue ( RedisQuery, TMVar Reply ) } -> QueryBuffer

newQueryBuffer :: IO QueryBuffer
newQueryBuffer = QueryBuffer <$> newTQueueIO

bufferRedisQuery :: QueryBuffer -> RedisQuery -> IO (TMVar Reply)
bufferRedisQuery (QueryBuffer queue) query = do
  resultVar <- newEmptyTMVarIO
  atomically $ writeTQueue queue (query, resultVar)
  return resultVar

runConsumer :: Connection -> QueryBuffer -> Int -> IO ()
runConsumer conn (QueryBuffer queue) batchSize =
  let consumeQueryBatch :: [(RedisQuery, TMVar Reply)] -> IO ()
      consumeQueryBatch queryBatch = do
        results <- runRedis conn (map fst queryBatch)
        traverse_ (uncurry handleResult) (zip queryBatch results)

      handleResult :: (RedisQuery, TMVar Reply) -> Reply -> IO ()
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

randomString :: Int -> IO ByteString
randomString len = pack . take len . randomRs ('a', 'z') <$> newStdGen

main :: IO ()
main = do
  -- connection pool
  conn <- connect defaultConnectInfo
  connCluster <- connect defaultConnectInfo{ connectPort = 30001, cluster = True }
  -- MVar to synchronize the client threads
  done <- newEmptyMVar
  -- Start the consumer thread
  queryBuffer <- newQueryBuffer
  let consumerAction = runConsumer conn queryBuffer (bufferSize myConfig)
  _ <- async consumerAction

  -- Run the benchmarks
  putStrLn "CPU time (with batching)"
  benchmark (requests myConfig * clients myConfig) $ bufferTest done queryBuffer
  putStrLn "CPU time (without batching)"
  benchmark (requests myConfig * clients myConfig) $ withoutBufferTest done conn
  putStrLn "CPU time (without batching) - Cluster"
  benchmark (requests myConfig * clients myConfig) $ withoutBufferTest done connCluster
  where
  myConfig :: Config
  myConfig = Config
    { clients = 5
    , requests = 2
    , bufferSize = 10
    }
  bufferTest :: MVar () -> QueryBuffer -> RedisQuery -> IO ()
  bufferTest done queryBuffer query = do
    replicateM_ (clients myConfig) $ forkIO $ do
      resultVars <- replicateM (requests myConfig) $ bufferRedisQuery queryBuffer query
      -- let's not worry about printing the results for now
      _ <- atomically $ mapM takeTMVar resultVars
      putMVar done ()
    replicateM_ (clients myConfig) $ takeMVar done
  withoutBufferTest :: MVar () -> Connection -> RedisQuery -> IO ()
  withoutBufferTest done conn query = do
    replicateM_ (clients myConfig) $ forkIO $ do
      replicateM_ (requests myConfig) $ runRedis conn [ query ]
      putMVar done ()
    replicateM_ (clients myConfig) $ takeMVar done
  benchmark :: Int -> ([ByteString] -> IO a) -> IO ()
  benchmark requestsPerBatch action = do
    let itrs = 100000
    let batchSize = 100
    let batches = chunksOf batchSize [1::Int .. itrs]
    let warmupRuns = 5

    -- Warm-up
    replicateM_ warmupRuns $ forM_ [1..batchSize] $ const (action ["SET", "key", "val"])

    -- Actual runs 
    batchTimes <- forM batches $ \batch -> do
      key <- randomString 10
      -- Force evaluation of key
      key `seq` return ()
      let !query = [ "SET", key, "val" ]
      tick <- getCPUTime
      forM_ batch $ const (action query)
      tock <- getCPUTime
      let timeInUs = fromIntegral (tock - tick) / 1000000 :: Double
      return timeInUs

    let avgTime = sum batchTimes / fromIntegral (length batchTimes)
    print $ "Average time per call: " ++ show (avgTime / fromIntegral (batchSize * requestsPerBatch)) ++ " us"
