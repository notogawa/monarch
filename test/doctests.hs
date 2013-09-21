import Test.DocTest

main :: IO ()
main = doctest [ "-isrc"
               , "Database.Monarch.Utils" ]
