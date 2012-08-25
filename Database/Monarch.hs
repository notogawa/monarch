{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | This module makes tokyotyrant-haskell monadic.
module Database.Monarch
    (
      -- * Basic Types
      Key, Value, Host, Port
      -- * TokyoTyrant DB action monad and its runner
    , Monarch, runMonarch
      -- * Fetch/store single key/value
    , get, put, putKeep
      -- * Fetch/store multiple key/value
    , mget, mput
      -- * Delete key/value
    , delete, vanish
      -- * Search key prefix
    , fwmkeys
    ) where

import qualified Database.TokyoTyrant.FFI as TT
import Data.ByteString
import Control.Applicative
import Control.Monad.Reader
import Control.Monad.Error
import Control.Exception

type Key = ByteString

type Value = ByteString

type Host = ByteString

type Port = Int

-- | A monad supporting TokyoTyrant access.
newtype Monarch a = Monarch { unMonarch :: ErrorT String (ReaderT TT.Connection IO) a}
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader TT.Connection, MonadError String )

-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch :: MonadIO m =>
              Host      -- ^ host
           -> Port      -- ^ port
           -> Monarch a -- ^ action
           -> m (Either String a)
runMonarch host port monarch = liftIO $ bracket open' close' action
    where
      action = (return . Left) `either` (runReaderT . runErrorT . unMonarch $ monarch)
      open' = TT.open host port
      close' = (const $ return ()) `either` TT.close

liftMonarch :: (MonadIO m, MonadError a m) => IO (Either a b) -> m b
liftMonarch action = liftIO action >>= either throwError return

-- | Get a value. If not found, return Nothing.
get :: Key -- ^ key
    -> Monarch (Maybe Value)
get key = do
  conn <- ask
  liftMonarch $ TT.get conn key

-- | Destructive store a value.
put :: Key   -- ^ key
    -> Value -- ^ value
    -> Monarch ()
put key value = do
  conn <- ask
  liftMonarch $ TT.put conn key value

-- | Non-Destructive store a value.
putKeep :: Key   -- ^ key
        -> Value -- ^ value
        -> Monarch ()
putKeep key value = do
  conn <- ask
  liftMonarch $ TT.putKeep conn key value

-- | Get values.
mget :: [Key] -- ^ keys to get
     -> Monarch [(Key, Value)]
mget keys = do
  conn <- ask
  liftMonarch $ TT.mget conn keys

-- | Put values.
mput :: [(Key, Value)] -- ^ key/value pairs to put
     -> Monarch ()
mput kvs = do
  conn <- ask
  liftMonarch $ TT.mput conn kvs

-- | Delete a value.
delete :: Key -- ^ key to delete
       -> Monarch ()
delete key = do
  conn <- ask
  liftMonarch $ TT.delete conn key

-- | Delete all value.
vanish :: Monarch ()
vanish = do
  conn <- ask
  liftMonarch $ TT.vanish conn

-- | Search keys by prefix.
fwmkeys :: ByteString -- ^ key prefix
        -> Maybe Int  -- ^ max # of returned keys;
                      --   Nothing means no limit
        -> Monarch [Key]
fwmkeys key n = do
  conn <- ask
  liftMonarch $ TT.fwmkeys conn key (maybe (-1) id n)
