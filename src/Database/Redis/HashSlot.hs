{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.Redis.HashSlot(HashSlot, hashSlotForKeys) where

import Data.Bits((.&.), xor, shiftL)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString as BS
import Data.Maybe (fromMaybe)
import Data.Word(Word8, Word16)
import Control.Exception (Exception, throwIO)
import Data.List (nub)

-- $setup
-- >>> :set -XOverloadedStrings

newtype HashSlot = HashSlot Word16 deriving (Num, Eq, Ord, Real, Enum, Integral, Show)

numHashSlots :: Word16
numHashSlots = 16384

hashSlotForKeys :: Exception e => e -> [BS.ByteString] -> IO HashSlot
hashSlotForKeys exception keys =
    case nub (keyToSlot <$> keys) of
        -- If none of the commands contain a key we can send them to any
        -- node. Let's pick the first one.
        [] -> return 0
        [hashSlot] -> return hashSlot
        _ -> throwIO exception

-- | Compute the hashslot associated with a key
--
-- >>> keyToSlot "123"
-- HashSlot 5970
-- >>> keyToSlot "{123"
-- HashSlot 2872
-- >>> keyToSlot "{123}"
-- HashSlot 5970
-- >>> keyToSlot "{}123"
-- HashSlot 7640
-- >>> keyToSlot "{123}1{abc}"
-- HashSlot 5970
-- >>> keyToSlot "\00\01"
-- HashSlot 4129
keyToSlot :: BS.ByteString -> HashSlot
keyToSlot = HashSlot . (.&.) (numHashSlots - 1) . crc16 . findSubKey

-- | Find the section of a key to compute the slot for.
findSubKey :: BS.ByteString -> BS.ByteString
findSubKey key = fromMaybe key (go key) where
  go bs = case Char8.break (=='{') bs of
    (_, "") -> Nothing
    (_, xs)  -> case Char8.break (=='}') (Char8.tail xs) of
      ("", _) -> go (Char8.tail xs)
      (_, "") -> Nothing
      (subKey, _) -> Just subKey

crc16 :: BS.ByteString -> Word16
crc16 = BS.foldl (crc16Update 0x1021) 0

-- Taken from crc16 package
crc16Update :: Word16  -- ^ polynomial
            -> Word16 -- ^ initial crc
            -> Word8 -- ^ data byte
            -> Word16 -- ^ new crc
crc16Update poly crc b = 
  foldl crc16UpdateBit newCrc [1 :: Int .. 8]
  where 
    newCrc = crc `xor` shiftL (fromIntegral b :: Word16) 8
    crc16UpdateBit crc' _ =
      if (crc' .&. 0x8000) /= 0x0000
          then shiftL crc' 1 `xor` poly
          else shiftL crc' 1
 
