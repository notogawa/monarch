{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | Types.
module Database.Monarch.Types
    (
      Monarch, runMonarch, liftMonarch
    , ExtOption(..), RestoreOption(..), MiscOption(..)
    , Code(..)
    ) where

import Data.IORef
import Data.ByteString
import Data.Conduit
import Data.Conduit.Network
import Control.Monad.Error
import Control.Applicative

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

-- | Lift
liftMonarch :: Pipe ByteString ByteString ByteString () IO a
            -> Monarch a
liftMonarch = Monarch . lift
