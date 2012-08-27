{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | This module makes TokyoTyrant binary protocol monadic.
--
--   <http://fallabs.com/tokyotyrant/spex.html#protocol>
module Database.Monarch
    (
      Monarch, runMonarch
    , ExtOption(..), RestoreOption(..), MiscOption(..)
    , Code(..)
    , put, putKeep, putCat, putShiftLeft
    , putNoResponse
    , out
    , get, multipleGet
    , valueSize
    , iterInit, iterNext
    , forwardMatchingKeys
    , addInt, addDouble
    , ext, sync, optimize, vanish, copy, restore
    , setMaster
    , recordNum, size
    , status
    , misc
    ) where

import Data.Bits
import Data.Int
import Data.Conduit
import Data.Conduit.Network
import qualified Data.Conduit.Binary as CB
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Control.Applicative
import Control.Monad.Reader
import Control.Monad.Error
import Data.IORef
import qualified Data.Binary as B
import Data.Binary.Put (runPut, putWord32be, putByteString)
import Data.Binary.Get (runGet, getWord32be)

type TTPipe =
    Pipe BS.ByteString BS.ByteString BS.ByteString () IO

-- | Error code
data Code = Success
          | InvalidOperation
          | HostNotFound
          | ConnectionRefused
          | SendError
          | ReceiveError
          | ExistingRecord
          | NoRecordFound
          | MiscellaneousError
            deriving (Eq, Show)

instance Error Code

class BitFlag32 a where
    fromOption :: a -> Int32

-- | Scripting extension options
data ExtOption = RecordLocking -- ^ record locking
               | GlobalLocking -- ^ global locking

instance BitFlag32 ExtOption where
    fromOption RecordLocking = 0x1
    fromOption GlobalLocking = 0x2

-- | Restore options
data RestoreOption = ConsistencyChecking -- ^ consistency checking

instance BitFlag32 RestoreOption where
    fromOption ConsistencyChecking = 0x1

-- | Miscellaneous operation options
data MiscOption = NoUpdateLog -- ^ omission of update log

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


-- | A monad supporting TokyoTyrant access.
newtype Monarch a =
    Monarch { unMonarch :: ErrorT Code TTPipe a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadError Code )

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              String
           -> Int
           -> Monarch a
           -> m (Either Code a)
runMonarch host port action =
    liftIO $ do
      result <- liftIO $ newIORef $ Left Success
      let remote = ClientSettings port host
      runTCPClient remote $ client action result
      readIORef result

client :: Monarch a
       -> IORef (Either Code a)
       -> Application IO
client action result src sink = src $$ conduit =$ sink
    where
      conduit = runErrorT (unMonarch action) >>=
                liftIO . writeIORef result

lengthBS32 :: BS.ByteString -> B.Word32
lengthBS32 = fromIntegral . BS.length

fromLBS :: LBS.ByteString -> BS.ByteString
fromLBS = BS.pack . LBS.unpack

liftMonarch :: TTPipe a
            -> Monarch a
liftMonarch = Monarch . lift

yieldRequest :: B.Put -> Monarch ()
yieldRequest = liftMonarch . yield . fromLBS . runPut

responseCode :: Monarch Code
responseCode =
    liftMonarch CB.head >>=
    maybe (throwError MiscellaneousError)
          (return . toCode . fromIntegral)

parseLBS :: Monarch LBS.ByteString
parseLBS = liftMonarch $
           CB.take 4 >>=
           CB.take . fromIntegral . runGet getWord32be

parseBS :: Monarch BS.ByteString
parseBS = fromLBS <$> parseLBS

parseWord32 :: Monarch B.Word32
parseWord32 = liftMonarch (CB.take 4) >>=
              return . runGet getWord32be

parseInt64 :: Monarch Int64
parseInt64 = liftMonarch (CB.take 8) >>=
             return . runGet (B.get :: B.Get Int64)

parseDouble :: Monarch Double
parseDouble = do
  integ <- fromIntegral <$> parseInt64
  fract <- fromIntegral <$> parseInt64
  return $ integ + fract * 1e-12

parseKeyValue :: Monarch (BS.ByteString, BS.ByteString)
parseKeyValue =
    liftMonarch $ do
      ksiz <- CB.take 4
      vsiz <- CB.take 4
      key <- CB.take . fromIntegral $
             runGet getWord32be ksiz
      value <- CB.take . fromIntegral $
               runGet getWord32be vsiz
      return (fromLBS key, fromLBS value)

communicate :: B.Put
            -> (Code -> Monarch a)
            -> Monarch a
communicate makeRequest makeResponse =
    yieldRequest makeRequest >>
    responseCode >>=
    makeResponse

-- | Store a record into a remote database object.
--   If a record with the same key exists in the database,
--   it is overwritten.
put :: BS.ByteString -- ^ key
    -> BS.ByteString -- ^ value
    -> Monarch ()
put key value = communicate request response
    where
      request = do
        putMagic 0x10
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putByteString key
        putByteString value
      response Success = return ()
      response code = throwError code

-- | Store a new record into a remote database object.
--   If a record with the same key exists in the database,
--   this function has no effect.
putKeep :: BS.ByteString -- ^ key
        -> BS.ByteString -- ^ value
        -> Monarch ()
putKeep key value = communicate request response
    where
      request = do
        putMagic 0x11
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putByteString key
        putByteString value
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Concatenate a value at the end of the existing record in a remote database object.
--   If there is no corresponding record, a new record is created.
putCat :: BS.ByteString -- ^ key
       -> BS.ByteString -- ^ value
       -> Monarch ()
putCat key value = communicate request response
    where
      request = do
        putMagic 0x12
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putByteString key
        putByteString value
      response Success = return ()
      response code = throwError code

-- | Concatenate a value at the end of the existing record and shift it to the left.
--   If there is no corresponding record, a new record is created.
putShiftLeft :: BS.ByteString -- ^ key
             -> BS.ByteString -- ^ value
             -> Int  -- ^ width
             -> Monarch ()
putShiftLeft key value width = communicate request response
    where
      request = do
        putMagic 0x13
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putWord32be $ fromIntegral width
        putByteString key
        putByteString value
      response Success = return ()
      response code = throwError code

-- | Store a record into a remote database object without response from the server.
--   If a record with the same key exists in the database, it is overwritten.
putNoResponse :: BS.ByteString -- ^ key
              -> BS.ByteString -- ^ value
              -> Monarch ()
putNoResponse key value = yieldRequest request
    where
      request = do
        putMagic 0x18
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putByteString key
        putByteString value

-- | Remove a record of a remote database object.
out :: BS.ByteString -- ^ key
    -> Monarch ()
out key = communicate request response
    where
      request = do
        putMagic 0x20
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Retrieve a record in a remote database object.
get :: BS.ByteString -- ^ key
    -> Monarch (Maybe BS.ByteString)
get key = communicate request response
    where
      request = do
        putMagic 0x30
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just <$> parseBS
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Retrieve records in a remote database object.
multipleGet :: [BS.ByteString] -- ^ keys
            -> Monarch [(BS.ByteString, BS.ByteString)]
multipleGet keys = communicate request response
    where
      request = do
        putMagic 0x31
        putWord32be $ fromIntegral $ length keys
        mapM_ (\key -> do
                 putWord32be $ lengthBS32 key
                 putByteString key) keys
      response Success = do
          siz <- fromIntegral <$> parseWord32
          sequence $ Prelude.replicate siz parseKeyValue
      response code = throwError code

-- | Get the size of the value of a record in a remote database object.
valueSize :: BS.ByteString -- ^ key
          -> Monarch (Maybe Int)
valueSize key = communicate request response
    where
      request = do
        putMagic 0x38
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just . fromIntegral <$> parseWord32
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Initialize the iterator of a remote database object.
--   The iterator is used in order to access the key of every record stored in a database.
iterInit :: Monarch ()
iterInit = communicate request response
    where
      request = putMagic 0x50
      response Success = return ()
      response code = throwError code

-- | Get the next key of the iterator of a remote database object.
--   The iterator can be updated by multiple connections and then it is not assured that every record is traversed.
iterNext :: Monarch (Maybe BS.ByteString)
iterNext = communicate request response
    where
      request = putMagic 0x51
      response Success = Just <$> parseBS
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Get forward matching keys in a remote database object.
forwardMatchingKeys :: BS.ByteString -- ^ key prefix
                    -> Int -- ^ maximum number of keys to be fetched
                    -> Monarch [BS.ByteString]
forwardMatchingKeys prefix n = communicate request response
    where
      request = do
        putMagic 0x58
        putWord32be $ lengthBS32 prefix
        putWord32be $ fromIntegral n
        putByteString prefix
      response Success = do
        siz <- fromIntegral <$> parseWord32
        sequence $ Prelude.replicate siz parseBS
      response code = throwError code

-- | Add an integer to a record in a remote database object.
--   If the corresponding record exists, the value is treated as an integer and is added to.
--   If no record corresponds, a new record of the additional value is stored.
addInt :: BS.ByteString -- ^ key
       -> Int -- ^ value
       -> Monarch Int
addInt key n = communicate request response
    where
      request = do
        putMagic 0x60
        putWord32be $ lengthBS32 key
        putWord32be $ fromIntegral n
        putByteString key
      response Success = fromIntegral <$> parseWord32
      response code = throwError code

-- | Add a real number to a record in a remote database object.
--   If the corresponding record exists, the value is treated as a real number and is added to.
--   If no record corresponds, a new record of the additional value is stored.
addDouble :: BS.ByteString -- ^ key
          -> Double -- ^ value
          -> Monarch Double
addDouble key n = communicate request response
    where
      request = do
        putMagic 0x61
        putWord32be $ lengthBS32 key
        B.put (truncate n :: Int64)
        B.put (truncate (snd (properFraction n :: (Int,Double)) * 1e12) :: Int64)
        putByteString key
      response Success = parseDouble
      response code = throwError code

-- | Call a function of the script language extension.
ext :: BS.ByteString -- ^ function
    -> [ExtOption] -- ^ option flags
    -> BS.ByteString -- ^ key
    -> BS.ByteString -- ^ value
    -> Monarch BS.ByteString
ext func opts key value = communicate request response
    where
      request = do
        putMagic 0x68
        putWord32be $ lengthBS32 func
        putOptions opts
        putWord32be $ lengthBS32 key
        putWord32be $ lengthBS32 value
        putByteString func
        putByteString key
        putByteString value
      response Success = parseBS
      response code = throwError code

-- | Synchronize updated contents of a remote database object with the file and the device.
sync :: Monarch ()
sync = communicate request response
    where
      request = putMagic 0x70
      response Success = return ()
      response code = throwError code

-- | Optimize the storage of a remove database object.
optimize :: BS.ByteString -- ^ parameter
         -> Monarch ()
optimize param = communicate request response
    where
      request = do
        putMagic 0x71
        putWord32be $ lengthBS32 param
        putByteString param
      response Success = return ()
      response code = throwError code

-- | Remove all records of a remote database object.
vanish :: Monarch ()
vanish = communicate request response
    where
      request = putMagic 0x72
      response Success = return ()
      response code = throwError code

-- | Copy the database file of a remote database object.
copy :: BS.ByteString -- ^ path
     -> Monarch ()
copy path = communicate request response
    where
      request = do
        putMagic 0x73
        putWord32be $ lengthBS32 path
        putByteString path
      response Success = return ()
      response code = throwError code

-- | Restore the database file of a remote database object from the update log.
restore :: Integral a =>
           BS.ByteString -- ^ path
        -> a -- ^ beginning time stamp in microseconds
        -> [RestoreOption] -- ^ option flags
        -> Monarch ()
restore path usec opts = communicate request response
    where
      request = do
        putMagic 0x74
        putWord32be $ lengthBS32 path
        B.put (fromIntegral usec :: Int64)
        putOptions opts
        putByteString path
      response Success = return ()
      response code = throwError code

-- | Set the replication master of a remote database object.
setMaster :: Integral a =>
             BS.ByteString -- ^ host
          -> Int -- ^ port
          -> a -- ^ beginning time stamp in microseconds
          -> [RestoreOption] -- ^ option flags
          -> Monarch ()
setMaster host port usec opts = communicate request response
    where
      request = do
        putMagic 0x78
        putWord32be $ lengthBS32 host
        putWord32be $ fromIntegral port
        B.put (fromIntegral usec :: Int64)
        putOptions opts
        putByteString host
      response Success = return ()
      response code = throwError code

-- | Get the number of records of a remote database object.
recordNum :: Monarch Int64
recordNum = communicate request response
    where
      request = putMagic 0x80
      response Success = parseInt64
      response code = throwError code

-- | Get the size of the database of a remote database object.
size :: Monarch Int64
size = communicate request response
    where
      request = putMagic 0x81
      response Success = parseInt64
      response code = throwError code

-- | Get the status string of the database of a remote database object.
status :: Monarch BS.ByteString
status = communicate request response
    where
      request = putMagic 0x88
      response Success = parseBS
      response code = throwError code

-- | Call a versatile function for miscellaneous operations of a remote database object.
misc :: BS.ByteString -- ^ function name
     -> [MiscOption] -- ^ option flags
     -> [BS.ByteString] -- ^ arguments
     -> Monarch [BS.ByteString]
misc func opts args = communicate request response
  where
    request = do
      putMagic 0x90
      putWord32be $ lengthBS32 func
      putOptions opts
      putWord32be $ fromIntegral $ length args
      putByteString func
      mapM_ putByteString args
    response Success = do
      siz <- fromIntegral <$> parseWord32
      sequence $ Prelude.replicate siz parseBS
    response code = throwError code
