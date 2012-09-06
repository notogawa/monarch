{-# LANGUAGE OverloadedStrings #-}
import Database.Monarch

import qualified Database.TokyoTyrant.FFI as FFI
import Control.Monad
import Data.ByteString.Char8 ()

main :: IO ()
main = monarch

ffi :: IO ()
ffi = do
  Right conn <- FFI.open "localhost" 1978
  forM_ [1..1000::Int] $ \_ -> do
    FFI.put conn "foo" "bar"
    FFI.get conn "foo"
    return ()
  FFI.close conn

monarch :: IO ()
monarch = do
  withMonarchConn "localhost" 1978 $ runMonarchConn $ do
         forM_ [1..1000::Int] $ \_ -> do
            put "foo" "bar"
            get "foo"
            return ()
  return ()
