{-# LANGUAGE OverloadedStrings #-}
import Database.Monarch

import Control.Applicative
import Control.Monad.IO.Class
import Data.List
import qualified Data.ByteString as BS
import Data.ByteString.Char8 ()
import Test.HUnit
import Test.Hspec.Monadic
import Test.Hspec.HUnit ()

main :: IO ()
main = hspec $ do
         describe "put" $ do
           it "store a record" casePutRecord
           it "overwrite a record if same key exists" casePutOverwriteRecord
         describe "mput" $ do
           it "store records" caseMputRecords
         describe "putkeep" $ do
           it "store a new record" casePutKeepNewRecord
           it "has no effect if same key exists" casePutKeepNoEffect
         describe "putcat" $ do
           it "concatenate a value at the end of the existing record" casePutCatRecord
           it "store a new record if there is no corresponding record" casePutCatNewRecord
         describe "putshl" $ do
           it "concatenate a value at the end of the existing record and shift it to the left" casePutShlRecord
           it "store a new record if there is no corresponding record" casePutShlNewRecord
         describe "putnr" $ do
           it "store a record" casePutNrRecord
           it "overwrite a record if same key exists" casePutNrOverwriteRecord
         describe "out" $ do
           it "remove a record" caseOutRecord
           it "no effect if same key not exists" caseOutNoEffect
         describe "mout" $ do
           it "remove records" caseMoutRecords
         describe "get" $ do
           it "retrieve a record" caseGetRecord
           it "retrieve large record" caseGetLargeRecord
         describe "mget" $ do
           it "retrieve records" caseMgetRecords
         describe "vsiz" $ do
           it "get the size of the value of a record" caseVsizRecord
         describe "iterinit" $ do
           it "initialize the iterator" caseIterinit
         describe "iternext" $ do
           it "get the next key of the iterator" caseIternext
           it "invalid if end iterator" caseIternextInvalid
         describe "fwmkeys" $ do
           it "get forward matching keys" caseFwmkeys

returns :: (Eq a, Show a) =>
           MonarchT IO a
        -> Either Code a
        -> IO ()
action `returns` expected = connTest >> poolTest
  where
  connTest = do result <- withMonarchConn "127.0.0.1" 1978 $ runMonarchConn $ do
                            vanish
                            result <- action
                            vanish
                            return result
                result @?= expected
  poolTest = do result <- withMonarchPool "127.0.0.1" 1978 20 $ runMonarchPool $ do
                            vanish
                            result <- action
                            vanish
                            return result
                result @?= expected

casePutRecord :: Assertion
casePutRecord =
    action `returns` Right (Just "bar")
    where
      action = do
        put "foo" "bar"
        get "foo"

casePutOverwriteRecord :: Assertion
casePutOverwriteRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        put "foo" "bar"
        put "foo" "hoge"
        get "foo"

caseMputRecords :: Assertion
caseMputRecords =
    action `returns` Right (Just "bob", Just "bar")
    where
      action = do
        multiplePut [("foo","bar"),("alice","bob")]
        bob <- get "alice"
        bar <- get "foo"
        return (bob, bar)

casePutKeepNewRecord :: Assertion
casePutKeepNewRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        putKeep "foo" "hoge"
        get "foo"

casePutKeepNoEffect :: Assertion
casePutKeepNoEffect =
    action `returns` Right (Just "bar")
    where
      action = do
        putKeep "foo" "bar"
        putKeep "foo" "hoge"
        get "foo"

casePutCatRecord :: Assertion
casePutCatRecord =
    action `returns` Right (Just "abracadabra")
    where
      action = do
        put "foo" "abra"
        putCat "foo" "cadabra"
        get "foo"

casePutCatNewRecord :: Assertion
casePutCatNewRecord =
    action `returns` Right (Just "cadabra")
    where
      action = do
        putCat "foo" "cadabra"
        get "foo"

casePutShlRecord :: Assertion
casePutShlRecord =
    action `returns` Right (Just "racadabra")
    where
      action = do
        put "foo" "abra"
        putShiftLeft "foo" "cadabra" 9
        get "foo"

casePutShlNewRecord :: Assertion
casePutShlNewRecord =
    action `returns` Right (Just "cadabra")
    where
      action = do
        putShiftLeft "foo" "cadabra" 4
        get "foo"

casePutNrRecord :: Assertion
casePutNrRecord =
    action `returns` Right (Just "bar")
    where
      action = do
        putNoResponse "foo" "bar"
        get "foo"

casePutNrOverwriteRecord :: Assertion
casePutNrOverwriteRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        putNoResponse "foo" "bar"
        putNoResponse "foo" "hoge"
        get "foo"

caseOutRecord :: Assertion
caseOutRecord =
    action `returns` Right (Just "bar", Nothing)
    where
      action = do
        put "foo" "bar"
        put "hoge" "fuga"
        out "hoge"
        stored <- get "foo"
        unstored <- get "hoge"
        return (stored, unstored)

caseOutNoEffect :: Assertion
caseOutNoEffect =
    action `returns` Right ()
    where
      action = do
        out "hoge"

caseMoutRecords :: Assertion
caseMoutRecords =
    action `returns` Right (Nothing, Nothing)
    where
      action = do
        put "foo" "bar"
        put "hoge" "fuga"
        multipleOut ["foo", "hoge"]
        bar <- get "foo"
        fuga <- get "hoge"
        return (bar, fuga)

caseGetRecord :: Assertion
caseGetRecord =
    action `returns` Right (Just "bar", Nothing)
    where
      action = do
        put "foo" "bar"
        stored <- get "foo"
        unstored <- get "bar"
        return (stored, unstored)

caseGetLargeRecord :: Assertion
caseGetLargeRecord = do
  content <- BS.concat . replicate 1024 <$> liftIO (BS.readFile "test/specs.hs")
  action content `returns` Right (Just content)
    where
      action content = do
        put "foo" content
        get "foo"

caseMgetRecords :: Assertion
caseMgetRecords =
    action `returns` Right [ ("foo", "bar")
                           , ("huga", "hoge")
                           , ("abra", "cadabra")
                           ]
    where
      action = do
        put "foo" "bar"
        put "huga" "hoge"
        put "abra" "cadabra"
        multipleGet [ "foo"
                    , "huga"
                    , "unstored"
                    , "abra"
                    ]

caseVsizRecord :: Assertion
caseVsizRecord =
    action `returns` Right (Just 3, Nothing)
    where
      action = do
        put "foo" "bar"
        stored <- valueSize "foo"
        unstored <- valueSize "bar"
        return (stored, unstored)

caseIterinit :: Assertion
caseIterinit =
    action `returns` Right True
    where
      action = do
        put "foo" "bar"
        put "fuga" "hoge"
        put "abra" "cadabra"
        iterInit
        key1 <- iterNext
        _ <- iterNext
        iterInit
        key2 <- iterNext
        return $ key1 == key2

caseIternext :: Assertion
caseIternext =
    action `returns` Right [ Just "abra", Just "foo", Just "fuga" ]
    where
      action = do
        put "foo" "bar"
        put "fuga" "hoge"
        put "abra" "cadabra"
        iterInit
        key1 <- iterNext
        key2 <- iterNext
        key3 <- iterNext
        return $ sort [key1, key2, key3]

caseIternextInvalid :: Assertion
caseIternextInvalid =
    action `returns` Right Nothing
    where
      action = do
        iterNext

caseFwmkeys :: Assertion
caseFwmkeys =
    action `returns` Right [ "abra", "abrac" ]
    where
      action = do
        put "abr" "acadabra"
        put "abra" "cadabra"
        put "abrac" "adabra"
        put "abraca" "dabra"
        sort <$> forwardMatchingKeys "abra" (Just 2)
