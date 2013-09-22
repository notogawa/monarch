{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- | Raw definitions.
module Database.Monarch.Mock.Types
    (
      MockT
    , MockDB, mockDB
    , newMockDB, emptyMockDB
    , runMock
    , TTValue(..)
    ) where

import Control.Concurrent.STM.TVar
import Control.Monad.Error
import Control.Monad.Reader
import Control.Monad.Base
import Control.Applicative
import Control.Monad.Trans.Control
import qualified Data.ByteString as BS
import qualified Data.Map as M

import Database.Monarch.Types ( Code )

data TTValue = TTString BS.ByteString
             | TTInt Int
             | TTDouble Double

-- | Connection with TokyoTyrant
data MockDB = MockDB { mockDB :: M.Map BS.ByteString TTValue }

-- | The Mock monad transformer to provide TokyoTyrant access.
newtype MockT m a =
    MockT { unMockT :: ErrorT Code (ReaderT (TVar MockDB) m) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader (TVar MockDB), MonadError Code, MonadBase base )

instance MonadTrans MockT where
    lift = MockT . lift . lift

instance MonadTransControl MockT where
    newtype StT MockT a = StMock { unStMock :: Either Code a }
    liftWith f = MockT . ErrorT . ReaderT $ (\r -> liftM Right (f $ \t -> liftM StMock (runReaderT (runErrorT (unMockT t)) r)))
    restoreT = MockT . ErrorT . ReaderT . const . liftM unStMock

instance MonadBaseControl base m => MonadBaseControl base (MockT m) where
    newtype StM (MockT m) a = StMMockT { unStMMockT :: ComposeSt MockT m a }
    liftBaseWith = defaultLiftBaseWith StMMockT
    restoreM = defaultRestoreM unStMMockT

emptyMockDB :: MockDB
emptyMockDB = MockDB { mockDB = M.empty }

newMockDB :: IO (TVar MockDB)
newMockDB = newTVarIO emptyMockDB

-- | Run Mock with TokyoTyrant at target host and port.
runMock :: MonadIO m =>
           MockT m a
        -> TVar MockDB
        -> m (Either Code a)
runMock action =
    runReaderT (runErrorT $ unMockT action)
