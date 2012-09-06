{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- | Raw definitions.
module Database.Monarch.Raw
    (
      Monarch
    , Connection, ConnectionPool
    , withMonarchConn
    , withMonarchPool
    , runMonarchConn
    , runMonarchPool
    , ExtOption(..), RestoreOption(..), MiscOption(..)
    , Code(..)
    , sendLBS, recvLBS
    ) where

import Data.Int
import Data.Conduit.Network
import Data.Conduit.Pool
import Control.Exception.Lifted as E
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

-- | A monad supporting TokyoTyrant access.
newtype Monarch a =
    Monarch { unMonarch :: ErrorT Code (ReaderT Connection IO) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader Connection, MonadError Code, MonadBase IO )

instance MonadBaseControl IO Monarch where
    newtype StM Monarch a = StMM { unStMM :: StM (ErrorT Code (ReaderT Connection IO)) a }
    liftBaseWith f = Monarch . liftBaseWith $ \runInBase -> f $ liftM StMM . runInBase . unMonarch
    restoreM = Monarch . restoreM . unStMM

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              Connection
           -> Monarch a
           -> m (Either Code a)
runMonarch conn action =
    liftIO $ runReaderT (runErrorT $ unMonarch action) conn

-- | Create a TokyoTyrant connection and run the given action.
-- Don't use the given 'Connection' outside the action.
withMonarchConn :: (MonadBaseControl IO m, MonadIO m) =>
                   String
                -> Int
                -> (Connection -> m a)
                -> m a
withMonarchConn host port f =
    E.bracket open' close' f
    where
      open' = liftIO (Connection <$> getSocket host port)
      close' = liftIO . sClose . connection

-- | Create a TokyoTyrant connection pool and run the given action.
-- Don't use the given 'ConnectionPool' outside the action.
withMonarchPool :: (MonadBaseControl IO m, MonadIO m) =>
                   String
                -> Int
                -> Int
                -> (ConnectionPool -> m a)
                -> m a
withMonarchPool host port size f =
    liftIO (createPool open' close' 1 20 size) >>= f
    where
      open' = Connection <$> getSocket host port
      close' = sClose . connection

-- | Run action with a connection.
runMonarchConn :: (MonadBaseControl IO m, MonadIO m) =>
                  Monarch a
               -> Connection
               -> m (Either Code a)
runMonarchConn action conn = runMonarch conn action

-- | Run action with a unused connection from the pool.
runMonarchPool :: (MonadBaseControl IO m, MonadIO m) =>
                  Monarch a
               -> ConnectionPool
               -> m (Either Code a)
runMonarchPool action pool = withResource pool (\conn -> runMonarch conn action)

throwError' :: Code -> SomeException -> Monarch a
throwError' e _ = throwError e

sendLBS :: LBS.ByteString -> Monarch ()
sendLBS lbs = do
  conn <- connection <$> ask
  liftIO (NSLBS.sendAll conn lbs) `E.catch` throwError' SendError

recvLBS :: Int64 -> Monarch LBS.ByteString
recvLBS n = do
  conn <- connection <$> ask
  lbs <- liftIO (NSLBS.recv conn n) `E.catch` throwError' SendError
  if n /= LBS.length lbs
    then throwError ReceiveError
    else return lbs
