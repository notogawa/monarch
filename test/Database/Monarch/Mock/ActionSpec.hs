{-# LANGUAGE OverloadedStrings #-}
module Database.Monarch.Mock.ActionSpec ( spec ) where

import Control.Applicative
import Control.Monad.IO.Class
import Data.List
import qualified Data.ByteString as BS
import Data.ByteString.Char8 ()
import Test.Hspec
import Database.Monarch
import Database.Monarch.Mock

spec :: Spec
spec = do
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
  -- describe "iterinit" $ do
  --   it "initialize the iterator" caseIterinit
  -- describe "iternext" $ do
  --   it "get the next key of the iterator" caseIternext
  --   it "invalid if end iterator" caseIternextInvalid
  describe "fwmkeys" $ do
    it "get forward matching keys" caseFwmkeys

returns :: (Eq a, Show a) =>
           MockT IO a
        -> Either Code a
        -> IO ()
action `returns` expected = mockTest
  where
  mockTest = do
    mdb <- newMockDB
    result <- runMock (do
                        vanish
                        result <- action
                        vanish
                        return result
                      ) mdb
    result `shouldBe` expected

casePutRecord :: IO ()
casePutRecord =
    action `returns` Right (Just "bar")
    where
      action = do
        put "foo" "bar"
        get "foo"

casePutOverwriteRecord :: IO ()
casePutOverwriteRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        put "foo" "bar"
        put "foo" "hoge"
        get "foo"

caseMputRecords :: IO ()
caseMputRecords =
    action `returns` Right (Just "bob", Just "bar")
    where
      action = do
        multiplePut [("foo","bar"),("alice","bob")]
        bob <- get "alice"
        bar <- get "foo"
        return (bob, bar)

casePutKeepNewRecord :: IO ()
casePutKeepNewRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        putKeep "foo" "hoge"
        get "foo"

casePutKeepNoEffect :: IO ()
casePutKeepNoEffect =
    action `returns` Right (Just "bar")
    where
      action = do
        putKeep "foo" "bar"
        putKeep "foo" "hoge"
        get "foo"

casePutCatRecord :: IO ()
casePutCatRecord =
    action `returns` Right (Just "abracadabra")
    where
      action = do
        put "foo" "abra"
        putCat "foo" "cadabra"
        get "foo"

casePutCatNewRecord :: IO ()
casePutCatNewRecord =
    action `returns` Right (Just "cadabra")
    where
      action = do
        putCat "foo" "cadabra"
        get "foo"

casePutShlRecord :: IO ()
casePutShlRecord =
    action `returns` Right (Just "racadabra")
    where
      action = do
        put "foo" "abra"
        putShiftLeft "foo" "cadabra" 9
        get "foo"

casePutShlNewRecord :: IO ()
casePutShlNewRecord =
    action `returns` Right (Just "cadabra")
    where
      action = do
        putShiftLeft "foo" "cadabra" 4
        get "foo"

casePutNrRecord :: IO ()
casePutNrRecord =
    action `returns` Right (Just "bar")
    where
      action = do
        putNoResponse "foo" "bar"
        get "foo"

casePutNrOverwriteRecord :: IO ()
casePutNrOverwriteRecord =
    action `returns` Right (Just "hoge")
    where
      action = do
        putNoResponse "foo" "bar"
        putNoResponse "foo" "hoge"
        get "foo"

caseOutRecord :: IO ()
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

caseOutNoEffect :: IO ()
caseOutNoEffect =
    action `returns` Right ()
    where
      action = do
        out "hoge"

caseMoutRecords :: IO ()
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

caseGetRecord :: IO ()
caseGetRecord =
    action `returns` Right (Just "bar", Nothing)
    where
      action = do
        put "foo" "bar"
        stored <- get "foo"
        unstored <- get "bar"
        return (stored, unstored)

caseGetLargeRecord :: IO ()
caseGetLargeRecord = do
  content <- BS.concat . replicate 1024 <$> liftIO (BS.readFile "monarch.cabal")
  action content `returns` Right (Just content)
    where
      action content = do
        put "foo" content
        get "foo"

caseMgetRecords :: IO ()
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

caseVsizRecord :: IO ()
caseVsizRecord =
    action `returns` Right (Just 3, Nothing)
    where
      action = do
        put "foo" "bar"
        stored <- valueSize "foo"
        unstored <- valueSize "bar"
        return (stored, unstored)

_caseIterinit :: IO ()
_caseIterinit =
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

_caseIternext :: IO ()
_caseIternext =
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

_caseIternextInvalid :: IO ()
_caseIternextInvalid =
    action `returns` Right Nothing
    where
      action = do
        iterNext

caseFwmkeys :: IO ()
caseFwmkeys =
    action `returns` Right [ "abra", "abrac" ]
    where
      action = do
        put "abr" "acadabra"
        put "abra" "cadabra"
        put "abrac" "adabra"
        put "abraca" "dabra"
        sort <$> forwardMatchingKeys "abra" (Just 2)
