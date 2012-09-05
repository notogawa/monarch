{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
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
    , liftMonarch
    ) where

import Data.IORef
import Data.ByteString
import Data.Conduit
import Data.Conduit.Network
import Data.Conduit.Pool
import Control.Exception.Lifted (bracket)
import Control.Monad.Error
import Control.Applicative
import Control.Monad.Trans.Control
import Network.Socket

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
    Monarch { unMonarch :: ErrorT Code (Pipe ByteString ByteString ByteString () IO) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadError Code )

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              Connection
           -> Monarch a
           -> m (Either Code a)
runMonarch conn action =
    liftIO $ do
      let c = connection conn
      result <- newIORef (Left Success)
      client action result (sourceSocket c) (sinkSocket c)
      readIORef result

client :: Monarch a
       -> IORef (Either Code a)
       -> Application IO
client action result src sink = src $$ conduit =$ sink
    where
      conduit = runErrorT (unMonarch action) >>=
                liftIO . writeIORef result

-- | Create a TokyoTyrant connection and run the given action.
-- Don't use the given 'Connection' outside the action.
withMonarchConn :: (MonadBaseControl IO m, MonadIO m) =>
                   String
                -> Int
                -> (Connection -> m a)
                -> m a
withMonarchConn host port f =
    bracket open' close' f
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

-- | Lift
liftMonarch :: Pipe ByteString ByteString ByteString () IO a
            -> Monarch a
liftMonarch = Monarch . lift
