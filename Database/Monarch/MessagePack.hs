{-# LANGUAGE FlexibleContexts #-}
-- | Store/Retrieve a MessagePackable record.
module Database.Monarch.MessagePack
     (
       put, putKeep
     , putNoResponse
     , get
     ) where

import qualified Data.MessagePack as MsgPack
import Data.ByteString
import qualified Data.ByteString.Lazy as Lazy
import Data.Binary.Put ( PutM
                       , putWord32be
                       , putByteString
                       , putLazyByteString )
import Control.Applicative
import Control.Monad.Error
import Control.Monad.Trans.Control

import Database.Monarch.Raw
import Database.Monarch.Utils

putKeyMsg :: ByteString
          -> Lazy.ByteString
          -> PutM ()
putKeyMsg key msg = do
  putWord32be $ lengthBS32 key
  putWord32be $ lengthLBS32 msg
  putByteString key
  putLazyByteString msg

-- | Store a record.
--   If a record with the same key exists in the database,
--   it is overwritten.
put :: ( MonadBaseControl IO m
       , MonadIO m
       , MsgPack.Packable a ) =>
       ByteString -- ^ key
    -> a -- ^ value
    -> MonarchT m ()
put key value = communicate request response
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x10
        putKeyMsg key msg
      response Success = return ()
      response code = throwError code

-- | Store a new record.
--   If a record with the same key exists in the database,
--   this function has no effect.
putKeep :: ( MonadBaseControl IO m
           , MonadIO m
           , MsgPack.Packable a ) =>
           ByteString -- ^ key
        -> a -- ^ value
        -> MonarchT m ()
putKeep key value = communicate request response
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x11
        putKeyMsg key msg
      response Success = return ()
      response InvalidOperation = return ()
      response code = throwError code

-- | Store a record without response.
--   If a record with the same key exists in the database, it is overwritten.
putNoResponse :: ( MonadBaseControl IO m
                 , MonadIO m
                 , MsgPack.Packable a) =>
                 ByteString -- ^ key
              -> a -- ^ value
              -> MonarchT m ()
putNoResponse key value = yieldRequest request
    where
      msg = MsgPack.pack value
      request = do
        putMagic 0x18
        putKeyMsg key msg

-- | Retrieve a record.
get :: ( MonadBaseControl IO m
       , MonadIO m
       , MsgPack.Unpackable a ) =>
       ByteString -- ^ key
    -> MonarchT m (Maybe a)
get key = communicate request response
    where
      request = do
        putMagic 0x30
        putWord32be $ lengthBS32 key
        putByteString key
      response Success = Just . MsgPack.unpack <$> parseLBS
      response InvalidOperation = return Nothing
      response code = throwError code
