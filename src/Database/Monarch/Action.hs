{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- |
-- Module      : Database.Monarch.Action
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- TokyoTyrant Original Binary Protocol(<http://fallabs.com/tokyotyrant/spex.html#protocol>).
--
module Database.Monarch.Action () where

import Data.Int
import Data.Maybe
import qualified Data.Binary as B
import Data.Binary.Put (putWord32be, putByteString)
import Data.ByteString.Char8 ()
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans.Control

import Database.Monarch.Types
import Database.Monarch.Utils

instance ( MonadBaseControl IO m, MonadIO m ) => MonadMonarch (MonarchT m) where

    put key value = communicate request response where
        request = do
          putMagic 0x10
          mapM_ (putWord32be . lengthBS32) [key, value]
          mapM_ putByteString [key, value]
        response Success = return ()
        response code = throwError code

    multiplePut [] = return ()
    multiplePut kvs = void $ misc "putlist" [] (kvs >>= \(k,v)->[k,v])

    putKeep key value = communicate request response where
        request = do
          putMagic 0x11
          mapM_ (putWord32be . lengthBS32) [key, value]
          mapM_ putByteString [key, value]
        response Success = return ()
        response InvalidOperation = return ()
        response code = throwError code

    putCat key value = communicate request response where
        request = do
          putMagic 0x12
          mapM_ (putWord32be . lengthBS32) [key, value]
          mapM_ putByteString [key, value]
        response Success = return ()
        response code = throwError code

    putShiftLeft key value width = communicate request response where
        request = do
          putMagic 0x13
          mapM_ (putWord32be . lengthBS32) [key, value]
          putWord32be $ fromIntegral width
          mapM_ putByteString [key, value]
        response Success = return ()
        response code = throwError code

    putNoResponse key value = yieldRequest request where
        request = do
          putMagic 0x18
          mapM_ (putWord32be . lengthBS32) [key, value]
          mapM_ putByteString [key, value]

    out key = communicate request response where
        request = do
          putMagic 0x20
          putWord32be $ lengthBS32 key
          putByteString key
        response Success = return ()
        response InvalidOperation = return ()
        response code = throwError code

    multipleOut [] = return ()
    multipleOut keys = void $ misc "outlist" [] keys

    get key = communicate request response where
        request = do
          putMagic 0x30
          putWord32be $ lengthBS32 key
          putByteString key
        response Success = Just <$> parseBS
        response InvalidOperation = return Nothing
        response code = throwError code

    multipleGet keys = communicate request response where
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

    valueSize key = communicate request response where
        request = do
          putMagic 0x38
          putWord32be $ lengthBS32 key
          putByteString key
        response Success = Just . fromIntegral <$> parseWord32
        response InvalidOperation = return Nothing
        response code = throwError code

    iterInit = communicate request response where
        request = putMagic 0x50
        response Success = return ()
        response code = throwError code

    iterNext = communicate request response where
        request = putMagic 0x51
        response Success = Just <$> parseBS
        response InvalidOperation = return Nothing
        response code = throwError code

    forwardMatchingKeys prefix n = communicate request response where
        request = do
          putMagic 0x58
          putWord32be $ lengthBS32 prefix
          putWord32be $ fromIntegral (fromMaybe (-1) n)
          putByteString prefix
        response Success = do
          siz <- fromIntegral <$> parseWord32
          replicateM siz parseBS
        response code = throwError code

    addInt key n = communicate request response where
        request = do
          putMagic 0x60
          putWord32be $ lengthBS32 key
          putWord32be $ fromIntegral n
          putByteString key
        response Success = fromIntegral <$> parseWord32
        response code = throwError code

    addDouble key n = communicate request response where
        request = do
          putMagic 0x61
          putWord32be $ lengthBS32 key
          B.put (truncate n :: Int64)
          B.put (truncate (snd (properFraction n :: (Int,Double)) * 1e12) :: Int64)
          putByteString key
        response Success = parseDouble
        response code = throwError code

    ext func opts key value = communicate request response where
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

    sync = communicate request response where
        request = putMagic 0x70
        response Success = return ()
        response code = throwError code

    optimize param = communicate request response where
        request = do
          putMagic 0x71
          putWord32be $ lengthBS32 param
          putByteString param
        response Success = return ()
        response code = throwError code

    vanish = communicate request response where
        request = putMagic 0x72
        response Success = return ()
        response code = throwError code

    copy path = communicate request response where
        request = do
          putMagic 0x73
          putWord32be $ lengthBS32 path
          putByteString path
        response Success = return ()
        response code = throwError code

    restore path usec opts = communicate request response where
        request = do
          putMagic 0x74
          putWord32be $ lengthBS32 path
          B.put (fromIntegral usec :: Int64)
          putOptions opts
          putByteString path
        response Success = return ()
        response code = throwError code

    setMaster host port usec opts = communicate request response where
        request = do
          putMagic 0x78
          putWord32be $ lengthBS32 host
          putWord32be $ fromIntegral port
          B.put (fromIntegral usec :: Int64)
          putOptions opts
          putByteString host
        response Success = return ()
        response code = throwError code

    recordNum = communicate request response where
        request = putMagic 0x80
        response Success = parseInt64
        response code = throwError code

    size = communicate request response where
        request = putMagic 0x81
        response Success = parseInt64
        response code = throwError code

    status = communicate request response where
        request = putMagic 0x88
        response Success = parseBS
        response code = throwError code

    misc func opts args = communicate request response where
        request = do
          putMagic 0x90
          putWord32be $ lengthBS32 func
          putOptions opts
          putWord32be . fromIntegral $ length args
          putByteString func
          mapM_ (\arg -> do
                   putWord32be $ lengthBS32 arg
                   putByteString arg) args
        response Success = do
          siz <- fromIntegral <$> parseWord32
          replicateM siz parseBS
        response code = throwError code
