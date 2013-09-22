{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import qualified Database.Monarch as Monarch
import qualified Database.TokyoTyrant.FFI as FFI
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
size = 10000

ffi :: IO ()
ffi = do
  Right conn <- FFI.open "127.0.0.1" 1978
  FFI.put conn "foo" "bar"
  FFI.close conn

mffi :: IO ()
mffi = do
  Right conn <- FFI.open "127.0.0.1" 1978
  FFI.mput conn $ replicate size ("foo", "bar")
  FFI.close conn

monarch :: IO ()
monarch = do
  Right () <- Monarch.withMonarchConn "127.0.0.1" 1978 $
              Monarch.runMonarchConn $
              Monarch.put "foo" "bar"
  return ()

mmonarch :: IO ()
mmonarch = do
  Right () <- Monarch.withMonarchConn "127.0.0.1" 1978 $ Monarch.runMonarchConn $
         Monarch.multiplePut $ replicate size ("foo", "bar")
  return ()
