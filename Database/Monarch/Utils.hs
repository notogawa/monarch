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

putMagic :: B.Word8
         -> B.Put
putMagic magic = B.putWord8 0xC8 >> B.putWord8 magic

putOptions :: BitFlag32 option =>
              [option]
           -> B.Put
putOptions = putWord32be . fromIntegral .
             foldl (.|.) 0 . map fromOption

lengthBS32 :: BS.ByteString -> B.Word32
lengthBS32 = fromIntegral . BS.length

lengthLBS32 :: LBS.ByteString -> B.Word32
lengthLBS32 = fromIntegral . LBS.length

fromLBS :: LBS.ByteString -> BS.ByteString
fromLBS = BS.pack . LBS.unpack

yieldRequest :: B.Put -> Monarch ()
yieldRequest = sendLBS . runPut

responseCode :: Monarch Code
responseCode = toCode . fromIntegral . runGet B.getWord8 <$> recvLBS 1

parseLBS :: Monarch LBS.ByteString
parseLBS = recvLBS 4 >>=
           recvLBS . fromIntegral . runGet getWord32be

parseBS :: Monarch BS.ByteString
parseBS = fromLBS <$> parseLBS

parseWord32 :: Monarch B.Word32
parseWord32 = runGet getWord32be <$> recvLBS 4

parseInt64 :: Monarch Int64
parseInt64 = runGet (B.get :: B.Get Int64) <$> recvLBS 8

parseDouble :: Monarch Double
parseDouble = do
  integ <- fromIntegral <$> parseInt64
  fract <- fromIntegral <$> parseInt64
  return $ integ + fract * 1e-12

parseKeyValue :: Monarch (BS.ByteString, BS.ByteString)
parseKeyValue = do
  ksiz <- recvLBS 4
  vsiz <- recvLBS 4
  key <- recvLBS . fromIntegral $
         runGet getWord32be ksiz
  value <- recvLBS . fromIntegral $
           runGet getWord32be vsiz
  return (fromLBS key, fromLBS value)

communicate :: B.Put
            -> (Code -> Monarch a)
            -> Monarch a
communicate makeRequest makeResponse =
    yieldRequest makeRequest >>
    responseCode >>=
    makeResponse
