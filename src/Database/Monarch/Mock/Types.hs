{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
-- |
-- Module      : Database.Monarch.Mock.Types
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- Type definitions.
--
module Database.Monarch.Mock.Types
    (
      MockT
    , MockDB, mockDB
    , newMockDB, emptyMockDB
    , runMock
    , TTValue(..)
    ) where

import Control.Concurrent.STM.TVar
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Base
import Control.Monad.Trans.Control
import qualified Data.ByteString as BS
import qualified Data.Map as M

import Database.Monarch.Types ( Code )

-- | KVS Value type
data TTValue = TTString BS.ByteString
             | TTInt Int
             | TTDouble Double

-- | Connection with TokyoTyrant
data MockDB = MockDB { mockDB :: M.Map BS.ByteString TTValue -- ^ DB
                     }

-- | The Mock monad transformer to provide TokyoTyrant access.
newtype MockT m a =
    MockT { unMockT :: ExceptT Code (ReaderT (TVar MockDB) m) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader (TVar MockDB), MonadError Code, MonadBase base )

instance MonadTrans MockT where
    lift = MockT . lift . lift

instance MonadTransControl MockT where
    type StT MockT a = Either Code a
    liftWith f = MockT . ExceptT . ReaderT $ (\r -> liftM Right (f $ \t -> runReaderT (runExceptT (unMockT t)) r))
    restoreT = MockT . ExceptT . ReaderT . const

instance MonadBaseControl base m => MonadBaseControl base (MockT m) where
    type StM (MockT m) a = ComposeSt MockT m a
    liftBaseWith = defaultLiftBaseWith
    restoreM = defaultRestoreM

-- | Empty mock DB
emptyMockDB :: MockDB
emptyMockDB = MockDB { mockDB = M.empty }

-- | Create mock DB
newMockDB :: IO (TVar MockDB)
newMockDB = newTVarIO emptyMockDB

-- | Run Mock with TokyoTyrant at target host and port.
runMock :: MonadIO m =>
           MockT m a
        -> TVar MockDB
        -> m (Either Code a)
runMock action =
    runReaderT (runExceptT $ unMockT action)
