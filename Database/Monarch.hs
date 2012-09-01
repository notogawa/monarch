{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | This module provide TokyoTyrant monadic access interface.
--
module Database.Monarch
    (
      module Database.Monarch.Types
    , module Database.Monarch.Binary
    ) where

import Database.Monarch.Types hiding (liftMonarch)
import Database.Monarch.Binary