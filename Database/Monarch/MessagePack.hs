-- | Store/Retrieve a MessagePackable record.
module Database.Monarch.MessagePack
     (
       put, putKeep
     , putNoResponse
     , get
     ) where

import qualified Data.MessagePack as MsgPack
import Data.ByteString
import Data.Binary.Put (putWord32be, putByteString, putLazyByteString)
import Control.Applicative
import Control.Monad.Error

import Database.Monarch.Types
import Database.Monarch.Utils

-- | Store a record.
--   If a record with the same key exists in the database,
--   it is overwritten.
put :: MsgPack.Packable a =>
       ByteString -- ^ key
    -> a -- ^ value
    -> Monarch ()
put key value = communicate request response
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x10
        putWord32be $ lengthBS32 key
        putWord32be $ lengthLBS32 msg
        putByteString key
        putLazyByteString msg
      response Success = return ()
      response code = throwError code

-- | Store a new record.
--   If a record with the same key exists in the database,
--   this function has no effect.
putKeep :: MsgPack.Packable a =>
           ByteString -- ^ key
        -> a -- ^ value
        -> Monarch ()
putKeep key value = communicate request response
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x11
        putWord32be $ lengthBS32 key
        putWord32be $ lengthLBS32 msg
        putByteString key
        putLazyByteString msg
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Store a record without response.
--   If a record with the same key exists in the database, it is overwritten.
putNoResponse :: MsgPack.Packable a =>
                 ByteString -- ^ key
              -> a -- ^ value
              -> Monarch ()
putNoResponse key value = yieldRequest request
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x18
        putWord32be $ lengthBS32 key
        putWord32be $ lengthLBS32 msg
        putByteString key
        putLazyByteString msg

-- | Retrieve a record.
get :: MsgPack.Unpackable a =>
       ByteString -- ^ key
    -> Monarch (Maybe a)
get key = communicate request response
    where
      request = do
        putMagic 0x30
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just . MsgPack.unpack <$> parseLBS
      response InvalidOperation = return Nothing
      response code = throwError code
