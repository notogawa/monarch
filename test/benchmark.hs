{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import qualified Database.Monarch as M
import Data.ByteString.Char8 ()
import Control.Monad

size :: Int
size = 10000

main :: IO ()
main = defaultMain
       [ bgroup "TokyoTyrant (multiple put)"
         [ bench "monarch" $ nfIO $ do
              Right () <- M.withMonarchConn "127.0.0.1" 1978 $ M.runMonarchConn $
                          M.multiplePut $ replicate size ("foo", "bar")
              return ()
         ]
       , bgroup "TokyoTyrant (put)"
         [ bench "monarch" $ nfIO $ do
              Right () <- M.withMonarchConn "127.0.0.1" 1978 $ M.runMonarchConn $
                          replicateM_ size $ M.put "foo" "bar"
              return ()
         ]
       ]
