{-# LANGUAGE FlexibleContexts #-}
-- | TokyoTyrant Original Binary Protocol(<http://fallabs.com/tokyotyrant/spex.html#protocol>).
module Database.Monarch.Binary
    (
      put, putKeep, putCat, putShiftLeft
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

import Data.Int
import Data.Maybe
import qualified Data.Binary as B
import Data.Binary.Put (putWord32be, putByteString)
import Data.ByteString hiding (length, copy)
import Control.Applicative
import Control.Monad
import Control.Monad.Error
import Control.Monad.Trans.Control

import Database.Monarch.Raw
import Database.Monarch.Utils

-- | Store a record.
--   If a record with the same key exists in the database,
--   it is overwritten.
put :: ( MonadBaseControl IO m
       , MonadIO m ) =>
       ByteString -- ^ key
    -> ByteString -- ^ value
    -> MonarchT m ()
put key value = communicate request response
    where
      request = do
        putMagic 0x10
        mapM_ (putWord32be . lengthBS32) [key, value]
        mapM_ putByteString [key, value]
      response Success = return ()
      response code = throwError code

-- | Store a new record.
--   If a record with the same key exists in the database,
--   this function has no effect.
putKeep :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           ByteString -- ^ key
        -> ByteString -- ^ value
        -> MonarchT m ()
putKeep key value = communicate request response
    where
      request = do
        putMagic 0x11
        mapM_ (putWord32be . lengthBS32) [key, value]
        mapM_ putByteString [key, value]
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Concatenate a value at the end of the existing record.
--   If there is no corresponding record, a new record is created.
putCat :: ( MonadBaseControl IO m
          , MonadIO m ) =>
          ByteString -- ^ key
       -> ByteString -- ^ value
       -> MonarchT m ()
putCat key value = communicate request response
    where
      request = do
        putMagic 0x12
        mapM_ (putWord32be . lengthBS32) [key, value]
        mapM_ putByteString [key, value]
      response Success = return ()
      response code = throwError code

-- | Concatenate a value at the end of the existing record and shift it to the left.
--   If there is no corresponding record, a new record is created.
putShiftLeft :: ( MonadBaseControl IO m
                , MonadIO m ) =>
                ByteString -- ^ key
             -> ByteString -- ^ value
             -> Int  -- ^ width
             -> MonarchT m ()
putShiftLeft key value width = communicate request response
    where
      request = do
        putMagic 0x13
        mapM_ (putWord32be . lengthBS32) [key, value]
        putWord32be $ fromIntegral width
        mapM_ putByteString [key, value]
      response Success = return ()
      response code = throwError code

-- | Store a record without response.
--   If a record with the same key exists in the database, it is overwritten.
putNoResponse :: ( MonadBaseControl IO m
                 , MonadIO m ) =>
                 ByteString -- ^ key
              -> ByteString -- ^ value
              -> MonarchT m ()
putNoResponse key value = yieldRequest request
    where
      request = do
        putMagic 0x18
        mapM_ (putWord32be . lengthBS32) [key, value]
        mapM_ putByteString [key, value]

-- | Remove a record.
out :: ( MonadBaseControl IO m
       , MonadIO m ) =>
       ByteString -- ^ key
    -> MonarchT m ()
out key = communicate request response
    where
      request = do
        putMagic 0x20
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Retrieve a record.
get :: ( MonadBaseControl IO m
       , MonadIO m ) =>
       ByteString -- ^ key
    -> MonarchT m (Maybe ByteString)
get key = communicate request response
    where
      request = do
        putMagic 0x30
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just <$> parseBS
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Retrieve records.
multipleGet :: ( MonadBaseControl IO m
               , MonadIO m ) =>
               [ByteString] -- ^ keys
            -> MonarchT m [(ByteString, ByteString)]
multipleGet keys = communicate request response
    where
      request = do
        putMagic 0x31
        putWord32be . fromIntegral $ length keys
        mapM_ (\key -> do
                 putWord32be $ lengthBS32 key
                 putByteString key) keys
      response Success = do
          siz <- fromIntegral <$> parseWord32
          replicateM siz parseKeyValue
      response code = throwError code

-- | Get the size of the value of a record.
valueSize :: ( MonadBaseControl IO m
             , MonadIO m ) =>
             ByteString -- ^ key
          -> MonarchT m (Maybe Int)
valueSize key = communicate request response
    where
      request = do
        putMagic 0x38
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just . fromIntegral <$> parseWord32
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Initialize the iterator.
iterInit :: ( MonadBaseControl IO m
            , MonadIO m ) =>
            MonarchT m ()
iterInit = communicate request response
    where
      request = putMagic 0x50
      response Success = return ()
      response code = throwError code

-- | Get the next key of the iterator.
--   The iterator can be updated by multiple connections and then it is not assured that every record is traversed.
iterNext :: ( MonadBaseControl IO m
            , MonadIO m ) =>
            MonarchT m (Maybe ByteString)
iterNext = communicate request response
    where
      request = putMagic 0x51
      response Success = Just <$> parseBS
      response InvalidOperation = return Nothing
      response code = throwError code

-- | Get forward matching keys.
forwardMatchingKeys :: ( MonadBaseControl IO m
                       , MonadIO m ) =>
                       ByteString -- ^ key prefix
                    -> Maybe Int -- ^ maximum number of keys to be fetched. 'Nothing' means unlimited.
                    -> MonarchT m [ByteString]
forwardMatchingKeys prefix n = communicate request response
    where
      request = do
        putMagic 0x58
        putWord32be $ lengthBS32 prefix
        putWord32be $ fromIntegral (fromMaybe (-1) n)
        putByteString prefix
      response Success = do
        siz <- fromIntegral <$> parseWord32
        replicateM siz parseBS
      response code = throwError code

-- | Add an integer to a record.
--   If the corresponding record exists, the value is treated as an integer and is added to.
--   If no record corresponds, a new record of the additional value is stored.
addInt :: ( MonadBaseControl IO m
          , MonadIO m ) =>
          ByteString -- ^ key
       -> Int -- ^ value
       -> MonarchT m Int
addInt key n = communicate request response
    where
      request = do
        putMagic 0x60
        putWord32be $ lengthBS32 key
        putWord32be $ fromIntegral n
        putByteString key
      response Success = fromIntegral <$> parseWord32
      response code = throwError code

-- | Add a real number to a record.
--   If the corresponding record exists, the value is treated as a real number and is added to.
--   If no record corresponds, a new record of the additional value is stored.
addDouble :: ( MonadBaseControl IO m
             , MonadIO m ) =>
             ByteString -- ^ key
          -> Double -- ^ value
          -> MonarchT m Double
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
ext :: ( MonadBaseControl IO m
       , MonadIO m ) =>
       ByteString -- ^ function
    -> [ExtOption] -- ^ option flags
    -> ByteString -- ^ key
    -> ByteString -- ^ value
    -> MonarchT m ByteString
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

-- | Synchronize updated contents with the file and the device.
sync :: ( MonadBaseControl IO m
        , MonadIO m ) =>
        MonarchT m ()
sync = communicate request response
    where
      request = putMagic 0x70
      response Success = return ()
      response code = throwError code

-- | Optimize the storage.
optimize :: ( MonadBaseControl IO m
            , MonadIO m ) =>
            ByteString -- ^ parameter
         -> MonarchT m ()
optimize param = communicate request response
    where
      request = do
        putMagic 0x71
        putWord32be $ lengthBS32 param
        putByteString param
      response Success = return ()
      response code = throwError code

-- | Remove all records.
vanish :: ( MonadBaseControl IO m
          , MonadIO m ) =>
          MonarchT m ()
vanish = communicate request response
    where
      request = putMagic 0x72
      response Success = return ()
      response code = throwError code

-- | Copy the database file.
copy :: ( MonadBaseControl IO m
        , MonadIO m ) =>
        ByteString -- ^ path
     -> MonarchT m ()
copy path = communicate request response
    where
      request = do
        putMagic 0x73
        putWord32be $ lengthBS32 path
        putByteString path
      response Success = return ()
      response code = throwError code

-- | Restore the database file from the update log.
restore :: ( MonadBaseControl IO m
           , MonadIO m
           , Integral a ) =>
           ByteString -- ^ path
        -> a -- ^ beginning time stamp in microseconds
        -> [RestoreOption] -- ^ option flags
        -> MonarchT m ()
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

-- | Set the replication master.
setMaster :: ( MonadBaseControl IO m
             , MonadIO m
             , Integral a ) =>
             ByteString -- ^ host
          -> Int -- ^ port
          -> a -- ^ beginning time stamp in microseconds
          -> [RestoreOption] -- ^ option flags
          -> MonarchT m ()
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

-- | Get the number of records.
recordNum :: ( MonadBaseControl IO m
             , MonadIO m ) =>
             MonarchT m Int64
recordNum = communicate request response
    where
      request = putMagic 0x80
      response Success = parseInt64
      response code = throwError code

-- | Get the size of the database.
size :: ( MonadBaseControl IO m
        , MonadIO m ) =>
        MonarchT m Int64
size = communicate request response
    where
      request = putMagic 0x81
      response Success = parseInt64
      response code = throwError code

-- | Get the status string of the database.
status :: ( MonadBaseControl IO m
          , MonadIO m ) =>
          MonarchT m ByteString
status = communicate request response
    where
      request = putMagic 0x88
      response Success = parseBS
      response code = throwError code

-- | Call a versatile function for miscellaneous operations.
misc :: ( MonadBaseControl IO m
        , MonadIO m ) =>
        ByteString -- ^ function name
     -> [MiscOption] -- ^ option flags
     -> [ByteString] -- ^ arguments
     -> MonarchT m [ByteString]
misc func opts args = communicate request response
  where
    request = do
      putMagic 0x90
      putWord32be $ lengthBS32 func
      putOptions opts
      putWord32be . fromIntegral $ length args
      putByteString func
      mapM_ putByteString args
    response Success = do
      siz <- fromIntegral <$> parseWord32
      replicateM siz parseBS
    response code = throwError code
