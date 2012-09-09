{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- | Raw definitions.
module Database.Monarch.Raw
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
    ) where

import Prelude hiding (catch)
import Data.Int
import Data.Conduit.Pool
import Control.Exception.Lifted
import Control.Monad.Error
import Control.Monad.Reader
import Control.Monad.Base
import Control.Applicative
import Control.Monad.Trans.Control
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as NSLBS
import qualified Data.ByteString.Lazy as LBS

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

instance Error Code

-- | Options for scripting extension
data ExtOption = RecordLocking -- ^ record locking
               | GlobalLocking -- ^ global locking

-- | Options for restore
data RestoreOption = ConsistencyChecking -- ^ consistency checking

-- | Options for miscellaneous operation
data MiscOption = NoUpdateLog -- ^ omission of update log

-- | The Monarch monad transformer to provide TokyoTyrant access.
newtype MonarchT m a =
    MonarchT { unMonarchT :: ErrorT Code (ReaderT Connection m) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader Connection, MonadError Code, MonadBase base )

instance MonadTrans MonarchT where
    lift = MonarchT . lift . lift

instance MonadTransControl MonarchT where
    newtype StT MonarchT a = StMonarch { unStMonarch :: Either Code a }
    liftWith f = MonarchT . ErrorT . ReaderT $ (\r -> liftM Right (f $ \t -> liftM StMonarch (runReaderT (runErrorT (unMonarchT t)) r)))
    restoreT = MonarchT . ErrorT . ReaderT . const . liftM unStMonarch

instance MonadBaseControl base m => MonadBaseControl base (MonarchT m) where
    newtype StM (MonarchT m) a = StMMonarchT { unStMMonarchT :: ComposeSt MonarchT m a }
    liftBaseWith = defaultLiftBaseWith StMMonarchT
    restoreM = defaultRestoreM unStMMonarchT

type Monarch = MonarchT IO

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              Connection
           -> MonarchT m a
           -> m (Either Code a)
runMonarch conn action =
    runReaderT (runErrorT $ unMonarchT action) conn

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
withMonarchPool host port size f =
    liftIO (createPool open' close' 1 20 size) >>= f
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

sendLBS :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           LBS.ByteString
        -> MonarchT m ()
sendLBS lbs = do
  conn <- connection <$> ask
  liftIO (NSLBS.sendAll conn lbs) `catch` throwError' SendError

recvLBS :: ( MonadBaseControl IO m
           , MonadIO m ) =>
           Int64
        -> MonarchT m LBS.ByteString
recvLBS n = do
  conn <- connection <$> ask
  lbs <- liftIO (NSLBS.recv conn n) `catch` throwError' ReceiveError
  if n /= LBS.length lbs
    then throwError ReceiveError
    else return lbs

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
