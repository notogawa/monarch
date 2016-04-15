{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- |
-- Module      : Database.Monarch.Types
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- Type definitions.
--
module Database.Monarch.Types
    (
      Monarch, MonarchT
    , Connection, ConnectionPool
    , withMonarchConn
    , withMonarchPool
    , runMonarchConn
    , runMonarchPool
    , ExtOption(..), RestoreOption(..), MiscOption(..)
    , Code(..)
    , sendLBS, recvLBS
    , MonadMonarch(..)
    ) where

import Prelude
import Data.Int
import Data.Pool
import Control.Exception.Lifted
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Base
import Control.Monad.Trans.Control
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS

-- | Connection with TokyoTyrant
data Connection = Connection { connection :: Socket }

-- | Connection pool with TokyoTyrant
type ConnectionPool = Pool Connection

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

-- instance Error Code

-- | Options for scripting extension
data ExtOption = RecordLocking -- ^ record locking
               | GlobalLocking -- ^ global locking

-- | Options for restore
data RestoreOption = ConsistencyChecking -- ^ consistency checking

-- | Options for miscellaneous operation
data MiscOption = NoUpdateLog -- ^ omission of update log

-- | The Monarch monad transformer to provide TokyoTyrant access.
newtype MonarchT m a =
    MonarchT { unMonarchT :: ExceptT Code (ReaderT Connection m) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader Connection, MonadError Code, MonadBase base )

instance MonadTrans MonarchT where
    lift = MonarchT . lift . lift

instance MonadTransControl MonarchT where
    type StT MonarchT a = Either Code a
    liftWith f = MonarchT . ExceptT . ReaderT $ (\r -> liftM Right (f $ \t -> runReaderT (runExceptT (unMonarchT t)) r))
    restoreT = MonarchT . ExceptT . ReaderT . const

instance MonadBaseControl base m => MonadBaseControl base (MonarchT m) where
    type StM (MonarchT m) a = ComposeSt MonarchT m a
    liftBaseWith = defaultLiftBaseWith
    restoreM = defaultRestoreM

-- | IO Specialized
type Monarch = MonarchT IO

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              Connection
           -> MonarchT m a
           -> m (Either Code a)
runMonarch conn action =
    runReaderT (runExceptT $ unMonarchT action) conn

-- | Create a TokyoTyrant connection and run the given action.
-- Don't use the given 'Connection' outside the action.
withMonarchConn :: ( MonadBaseControl IO m
                   , MonadIO m ) =>
                   String -- ^ host
                -> Int -- ^ port
                -> (Connection -> m a)
                -> m a
withMonarchConn host port = bracket open' close'
    where
      open' = liftIO $ getConnection host port
      close' = liftIO . sClose . connection

-- | Create a TokyoTyrant connection pool and run the given action.
-- Don't use the given 'ConnectionPool' outside the action.
withMonarchPool :: ( MonadBaseControl IO m
                   , MonadIO m ) =>
                   String -- ^ host
                -> Int -- ^ port
                -> Int -- ^ number of connections
                -> (ConnectionPool -> m a)
                -> m a
withMonarchPool host port connections f =
    liftIO (createPool open' close' 1 20 connections) >>= f
    where
      open' = getConnection host port
      close' = sClose . connection

-- | Run action with a connection.
runMonarchConn :: ( MonadBaseControl IO m
                  , MonadIO m ) =>
                  MonarchT m a -- ^ action
               -> Connection -- ^ connection
               -> m (Either Code a)
runMonarchConn action conn = runMonarch conn action

-- | Run action with a unused connection from the pool.
runMonarchPool :: ( MonadBaseControl IO m
                  , MonadIO m ) =>
                  MonarchT m a -- ^ action
               -> ConnectionPool -- ^ connection pool
               -> m (Either Code a)
runMonarchPool action pool =
    withResource pool $ flip runMonarch action

throwError' :: Monad m =>
             Code
          -> SomeException
          -> MonarchT m a
throwError' = const . throwError

-- | Send.
sendLBS :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           LBS.ByteString
        -> MonarchT m ()
sendLBS lbs = do
  conn <- connection <$> ask
  liftIO (LBS.sendAll conn lbs) `catch` throwError' SendError

-- | Receive.
recvLBS :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           Int64
        -> MonarchT m LBS.ByteString
recvLBS n = do
  conn <- connection <$> ask
  lbs <- liftIO (LBS.recv conn n) `catch` throwError' ReceiveError
  if LBS.null lbs
    then throwError ReceiveError
    else if n == LBS.length lbs
           then return lbs
           else LBS.append lbs <$> recvLBS (n - LBS.length lbs)

-- | Make connection from host and port.
getConnection :: HostName
              -> Int
              -> IO Connection
getConnection host port = do
  let hints = defaultHints { addrFlags = [ AI_ADDRCONFIG ]
                           , addrSocketType = Stream
                           }
  (addr:_) <- getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  let failConnect = (\e -> sClose sock >> throwIO e) :: SomeException -> IO ()
  connect sock (addrAddress addr) `catch` failConnect
  return $ Connection sock

-- | Monad Monarch interfaces
class Monad m => MonadMonarch m where

  -- | Store a record.
  --   If a record with the same key exists in the database,
  --   it is overwritten.
  put :: BS.ByteString -- ^ key
      -> BS.ByteString -- ^ value
      -> m ()

  -- | Store records.
  --   If a record with the same key exists in the database,
  --   it is overwritten.
  multiplePut :: [(BS.ByteString,BS.ByteString)] -- ^ key & value pairs
              -> m ()

  -- | Store a new record.
  --   If a record with the same key exists in the database,
  --   this function has no effect.
  putKeep :: BS.ByteString -- ^ key
          -> BS.ByteString -- ^ value
          -> m ()

  -- | Concatenate a value at the end of the existing record.
  --   If there is no corresponding record, a new record is created.
  putCat :: BS.ByteString -- ^ key
         -> BS.ByteString -- ^ value
         -> m ()

  -- | Concatenate a value at the end of the existing record and shift it to the left.
  --   If there is no corresponding record, a new record is created.
  putShiftLeft :: BS.ByteString -- ^ key
               -> BS.ByteString -- ^ value
               -> Int  -- ^ width
               -> m ()

  -- | Store a record without response.
  --   If a record with the same key exists in the database, it is overwritten.
  putNoResponse :: BS.ByteString -- ^ key
                -> BS.ByteString -- ^ value
                -> m ()

  -- | Remove a record.
  out :: BS.ByteString -- ^ key
      -> m ()

  -- | Remove records.
  multipleOut :: [BS.ByteString] -- ^ keys
              -> m ()

  -- | Retrieve a record.
  get :: BS.ByteString -- ^ key
      -> m (Maybe BS.ByteString)

  -- | Retrieve records.
  multipleGet :: [BS.ByteString] -- ^ keys
              -> m [(BS.ByteString, BS.ByteString)]

  -- | Get the size of the value of a record.
  valueSize :: BS.ByteString -- ^ key
            -> m (Maybe Int)

  -- | Initialize the iterator.
  iterInit :: m ()

  -- | Get the next key of the iterator.
  --   The iterator can be updated by multiple connections and then it is not assured that every record is traversed.
  iterNext :: m (Maybe BS.ByteString)

  -- | Get forward matching keys.
  forwardMatchingKeys :: BS.ByteString -- ^ key prefix
                      -> Maybe Int -- ^ maximum number of keys to be fetched. 'Nothing' means unlimited.
                      -> m [BS.ByteString]

  -- | Add an integer to a record.
  --   If the corresponding record exists, the value is treated as an integer and is added to.
  --   If no record corresponds, a new record of the additional value is stored.
  addInt :: BS.ByteString -- ^ key
         -> Int -- ^ value
         -> m Int

  -- | Add a real number to a record.
  --   If the corresponding record exists, the value is treated as a real number and is added to.
  --   If no record corresponds, a new record of the additional value is stored.
  addDouble :: BS.ByteString -- ^ key
            -> Double -- ^ value
            -> m Double

  -- | Call a function of the script language extension.
  ext :: BS.ByteString -- ^ function
      -> [ExtOption] -- ^ option flags
      -> BS.ByteString -- ^ key
      -> BS.ByteString -- ^ value
      -> m BS.ByteString

  -- | Synchronize updated contents with the file and the device.
  sync :: m ()

  -- | Optimize the storage.
  optimize :: BS.ByteString -- ^ parameter
           -> m ()

  -- | Remove all records.
  vanish :: m ()

  -- | Copy the database file.
  copy :: BS.ByteString -- ^ path
       -> m ()

  -- | Restore the database file from the update log.
  restore :: ( Integral a ) =>
             BS.ByteString -- ^ path
          -> a -- ^ beginning time stamp in microseconds
          -> [RestoreOption] -- ^ option flags
          -> m ()

  -- | Set the replication master.
  setMaster :: ( Integral a ) =>
               BS.ByteString -- ^ host
            -> Int -- ^ port
            -> a -- ^ beginning time stamp in microseconds
            -> [RestoreOption] -- ^ option flags
            -> m ()

  -- | Get the number of records.
  recordNum :: m Int64

  -- | Get the size of the database.
  size :: m Int64

  -- | Get the status string of the database.
  status :: m BS.ByteString

  -- | Call a versatile function for miscellaneous operations.
  misc :: BS.ByteString -- ^ function name
       -> [MiscOption] -- ^ option flags
       -> [BS.ByteString] -- ^ arguments
       -> m [BS.ByteString]
