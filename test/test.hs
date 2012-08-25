{-# LANGUAGE OverloadedStrings #-}
import Database.Monarch

import Test.HUnit
import Test.Hspec.Monadic
import Test.Hspec.HUnit ()

main :: IO ()
main = hspec $ do
         describe "TokyoTyrant" $ do
                it "get unstored value" caseGetUnstored
                it "get already stored value" caseGetStored

caseGetUnstored :: Assertion
caseGetUnstored = do
  result <- runMonarch "localhost" 1978 $ do
                    vanish
                    result <- get "foo"
                    vanish
                    return result
  result @?= Right Nothing

caseGetStored :: Assertion
caseGetStored = do
  result <- runMonarch "localhost" 1978 $ do
                  vanish
                  put "foo" "bar"
                  result <- get "foo"
                  vanish
                  return result
  result @?= Right (Just "bar")
