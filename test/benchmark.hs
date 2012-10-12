{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import qualified Database.Monarch as Monarch
import qualified Database.TokyoTyrant.FFI as FFI
import Control.Monad
import Data.ByteString.Char8 ()

main :: IO ()
main = defaultMain
       [ bgroup "TokyoTyrant (multiple put)"
         [ bench "monarch" $ nfIO mmonarch
         , bench "tokyotyrant-haskell" $ nfIO mffi
         ]
       , bgroup "TokyoTyrant (put)"
         [ bench "monarch" $ nfIO monarch
         , bench "tokyotyrant-haskell" $ nfIO ffi
         ]
       ]

size :: Int
size = 30000

ffi :: IO ()
ffi = do
  Right conn <- FFI.open "localhost" 1978
  forM_ [1..size] $ \_ -> do
    FFI.put conn "foo" "bar"
    return ()
  FFI.close conn

mffi :: IO ()
mffi = do
  Right conn <- FFI.open "localhost" 1978
  FFI.mput conn $ replicate size ("foo", "bar")
  FFI.close conn

monarch :: IO ()
monarch = do
  Right () <- Monarch.withMonarchConn "localhost" 1978 $ Monarch.runMonarchConn $
    forM_ [1..size] $ \_ -> do
      Monarch.put "foo" "bar"
      return ()
  return ()

mmonarch :: IO ()
mmonarch = do
  Right () <- Monarch.withMonarchConn "localhost" 1978 $ Monarch.runMonarchConn $
         Monarch.multiplePut $ replicate size ("foo", "bar")
  return ()
