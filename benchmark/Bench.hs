{-# LANGUAGE GADTs, OverloadedStrings, BangPatterns #-}
-- queryQueue is unused and I am not comfortable seeing that warning.
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
import Control.Monad ( replicateM_, forM_, forM )
import Redis_simple.Database.Redis as Simple
import Hedis.Database.Redis as Hedis
import Data.ByteString (ByteString)
import Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar, MVar)
import System.CPUTime (getCPUTime)
import Data.List.Split (chunksOf)
import System.Random (newStdGen, Random (randomRs))
import Data.ByteString.Char8 (pack)

randomString :: Int -> IO ByteString
randomString len = pack . take len . randomRs ('a', 'z') <$> newStdGen

data Config = Config
  { clients :: Int
  , requests :: Int
  }

main :: IO ()
main = do
  -- redis-simple connection pool
  simpleConn <- Simple.connect Simple.defaultConnectInfo
  simpleConnCluster <- Simple.connect Simple.defaultConnectInfo{ Simple.connectPort = 30001, Simple.cluster = True }
  -- hedis connection pool
  hedisConn <- Hedis.connect Hedis.defaultConnectInfo
  hedisConnCluster <- Hedis.connectCluster Hedis.defaultConnectInfo{ Hedis.connectPort = Hedis.PortNumber 30001 }
  -- MVar to synchronize the client threads
  done <- newEmptyMVar

  -- Run the benchmarks
  putStrLn "CPU time (redis-simple: NonClustered)"
  benchmark (requests myConfig * clients myConfig) $ redisSimpleTest done simpleConn
  putStrLn "CPU time (redis-simpl: Clustered)"
  benchmark (requests myConfig * clients myConfig) $ redisSimpleTest done simpleConnCluster
  putStrLn "CPU time (hedis: NonClustered)"
  benchmark (requests myConfig * clients myConfig) $ hedisTest done hedisConn
  putStrLn "CPU time (hedis: Clustered)"
  benchmark (requests myConfig * clients myConfig) $ hedisTest done hedisConnCluster
  where
  myConfig :: Config
  myConfig = Config
    { clients = 5
    , requests = 2
    }
  redisSimpleTest :: MVar () -> Simple.Connection -> [ ByteString ] -> IO ()
  redisSimpleTest done conn query = do
    replicateM_ (clients myConfig) $ forkIO $ do
      replicateM_ (requests myConfig) $ Simple.runRedis conn [ query ]
      putMVar done ()
    replicateM_ (clients myConfig) $ takeMVar done
  hedisTest :: MVar () -> Hedis.Connection -> [ ByteString ] -> IO ()
  hedisTest done conn query = do
    let key = query !! 1
        val = query !! 2
    replicateM_ (clients myConfig) $ forkIO $ do
      replicateM_ (requests myConfig) $ Hedis.runRedis conn $ Hedis.set key val
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
