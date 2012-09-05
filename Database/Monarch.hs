{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | This module provide TokyoTyrant monadic access interface.
--
module Database.Monarch
    (
      module Database.Monarch.Raw
    , module Database.Monarch.Binary
    ) where

import Database.Monarch.Raw hiding (liftMonarch)
import Database.Monarch.Binary