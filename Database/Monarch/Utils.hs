{-# LANGUAGE FlexibleContexts #-}
module Database.Monarch.Utils
    (
      toCode
    , putMagic, putOptions
    , lengthBS32, lengthLBS32
    , fromLBS
    , yieldRequest
    , responseCode
    , parseLBS, parseBS
    , parseWord32, parseInt64, parseDouble
    , parseKeyValue
    , communicate
    ) where

import Data.Int
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as B
import Data.Binary.Put (runPut, putWord32be)
import Data.Binary.Get (runGet, getWord32be)
import Control.Applicative
import Control.Monad.IO.Class
import Control.Monad.Trans.Control

import Database.Monarch.Raw

class BitFlag32 a where
    fromOption :: a -> Int32

instance BitFlag32 ExtOption where
    fromOption RecordLocking = 0x1
    fromOption GlobalLocking = 0x2

instance BitFlag32 RestoreOption where
    fromOption ConsistencyChecking = 0x1

instance BitFlag32 MiscOption where
    fromOption NoUpdateLog = 0x1

toCode :: Int -> Code
toCode 0 = Success
toCode 1 = InvalidOperation
toCode 2 = HostNotFound
toCode 3 = ConnectionRefused
toCode 4 = SendError
toCode 5 = ReceiveError
toCode 6 = ExistingRecord
toCode 7 = NoRecordFound
toCode 9999 = MiscellaneousError
toCode _ = error "Invalid Code"

-- | TokyoTyrant Original Binary Protocal magic id.
--
-- Example:
--
-- >>> :m +Data.ByteString.Char8
-- >>> :set -XOverloadedStrings
-- >>> fromLBS (runPut $ putMagic 0x10) == "\xC8\x10"
-- True
--
putMagic :: B.Word8 -> B.Put
putMagic magic = B.putWord8 0xC8 >> B.putWord8 magic

-- | Option
--
-- Example:
--
-- >>> :m +Data.ByteString.Char8
-- >>> :set -XOverloadedStrings
-- >>> fromLBS (runPut $ putOptions [RecordLocking]) == "\0\0\0\1"
-- True
-- >>> fromLBS (runPut $ putOptions [GlobalLocking]) == "\0\0\0\2"
-- True
-- >>> fromLBS (runPut $ putOptions [RecordLocking, GlobalLocking]) == "\0\0\0\3"
-- True
--
putOptions :: BitFlag32 option =>
              [option]
           -> B.Put
putOptions = putWord32be . fromIntegral .
             foldl (.|.) 0 . map fromOption

-- | Get Length
--
-- Example:
--
-- >>> :m +Data.ByteString.Char8
-- >>> :set -XOverloadedStrings
-- >>> lengthBS32 "test"
-- 4
-- >>> lengthBS32 ""
-- 0
--
lengthBS32 :: BS.ByteString -> B.Word32
lengthBS32 = fromIntegral . BS.length

-- | Get Length
--
-- Example:
--
-- >>> :m +Data.ByteString.Lazy.Char8
-- >>> :set -XOverloadedStrings
-- >>> lengthLBS32 "test"
-- 4
-- >>> lengthLBS32 ""
-- 0
--
lengthLBS32 :: LBS.ByteString -> B.Word32
lengthLBS32 = fromIntegral . LBS.length

-- | Convert
--
-- Example:
--
-- >>> :m +Data.ByteString.Lazy.Char8
-- >>> :set -XOverloadedStrings
-- >>> fromLBS "test"
-- "test"
--
fromLBS :: LBS.ByteString -> BS.ByteString
fromLBS = BS.pack . LBS.unpack

yieldRequest :: ( MonadBaseControl IO m
                , MonadIO m ) =>
                B.Put
             -> MonarchT m ()
yieldRequest = sendLBS . runPut

responseCode :: ( MonadBaseControl IO m
                , MonadIO m ) =>
                MonarchT m Code
responseCode = toCode . fromIntegral . runGet B.getWord8 <$> recvLBS 1

parseLBS :: ( MonadBaseControl IO m
            , MonadIO m ) =>
            MonarchT m LBS.ByteString
parseLBS = recvLBS 4 >>=
           recvLBS . fromIntegral . runGet getWord32be

parseBS :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           MonarchT m BS.ByteString
parseBS = fromLBS <$> parseLBS

parseWord32 :: ( MonadBaseControl IO m
               , MonadIO m ) =>
               MonarchT m B.Word32
parseWord32 = runGet getWord32be <$> recvLBS 4

parseInt64 :: ( MonadBaseControl IO m
              , MonadIO m ) =>
              MonarchT m Int64
parseInt64 = runGet (B.get :: B.Get Int64) <$> recvLBS 8

parseDouble :: ( MonadBaseControl IO m
               , MonadIO m ) =>
               MonarchT m Double
parseDouble = do
  integ <- fromIntegral <$> parseInt64
  fract <- fromIntegral <$> parseInt64
  return $ integ + fract * 1e-12

parseKeyValue :: ( MonadBaseControl IO m
                 , MonadIO m ) =>
                 MonarchT m (BS.ByteString, BS.ByteString)
parseKeyValue = do
  ksiz <- recvLBS 4
  vsiz <- recvLBS 4
  key <- recvLBS . fromIntegral $
         runGet getWord32be ksiz
  value <- recvLBS . fromIntegral $
           runGet getWord32be vsiz
  return (fromLBS key, fromLBS value)

communicate :: ( MonadBaseControl IO m
               , MonadIO m) =>
               B.Put
            -> (Code -> MonarchT m a)
            -> MonarchT m a
communicate makeRequest makeResponse =
    yieldRequest makeRequest >>
    responseCode >>=
    makeResponse
