(ns yadat.where
  (:require [yadat.db :as db]
            [yadat.util :as util]
            [yadat.relation :as r]))

(declare resolve-clauses)

(defmulti resolve-clause
  "Resolves `clause` into a relation based on `db` and `relations`.
  Returns a list of relations of which the first relation must be the resolved
  relation. We cannot just return the resolved relation as there are clauses
  that modify the input `relations`."
  (fn [db relations clause] (clause-type clause)))
