{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.Monarch ( Monarch, Key, Value, Host, Port
                        , runMonarch
                        , get, put, putKeep
                        , mget, mput
                        , delete, vanish
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

newtype Monarch a = Monarch { unMonarch :: ErrorT String (ReaderT TT.Connection IO) a}
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader TT.Connection, MonadError String )

runMonarch :: MonadIO m => Host -> Port -> Monarch a -> m (Either String a)
runMonarch host port monarch = liftIO $ bracket open' close' action
    where
      action = (return . Left) `either` (runReaderT . runErrorT . unMonarch $ monarch)
      open' = TT.open host port
      close' = (const $ return ()) `either` TT.close

liftMonarch :: (MonadIO m, MonadError a m) => IO (Either a b) -> m b
liftMonarch action = liftIO action >>= either throwError return

get :: Key -> Monarch (Maybe Value)
get key = do
  conn <- ask
  liftMonarch $ TT.get conn key

put :: Key -> Value -> Monarch ()
put key value = do
  conn <- ask
  liftMonarch $ TT.put conn key value

putKeep :: Key -> Value -> Monarch ()
putKeep key value = do
  conn <- ask
  liftMonarch $ TT.putKeep conn key value

mget :: [Key] -> Monarch [(Key, Value)]
mget keys = do
  conn <- ask
  liftMonarch $ TT.mget conn keys

mput :: [(Key, Value)] -> Monarch ()
mput kvs = do
  conn <- ask
  liftMonarch $ TT.mput conn kvs

delete :: Key -> Monarch ()
delete key = do
  conn <- ask
  liftMonarch $ TT.delete conn key

vanish :: Monarch ()
vanish = do
  conn <- ask
  liftMonarch $ TT.vanish conn

fwmkeys :: ByteString -> Int -> Monarch [Value]
fwmkeys key n = do
  conn <- ask
  liftMonarch $ TT.fwmkeys conn key n
