import Test.DocTest

main :: IO ()
main = doctest [ "-Lcabal-dev/lib"
               , "-package-conf=cabal-dev/packages-7.4.2.conf"
               , "Database/Monarch/Utils.hs" ]
