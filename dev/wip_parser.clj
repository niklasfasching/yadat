(ns yadat.dev.wip-parser)
;; also
;; https://github.com/punitrathore/clj-parser/blob/master/src/clj_parser/core.clj

;; (defrecord Either [value error])

;; (defn any [input]
;;   (if-let [[first & rest] input]
;;     [first rest]
;;     []))

;; (defn <|>
;;   "orElse
;;   Try `parser1` and if it fails try `parser2`."
;;   [parser1 parser2]
;;   (fn [input]
;;     (let [result1 (parser1 input)]
;;       (if (:error result1)
;;         (parser2 input)
;;         result1))))

;; (defn >>=
;;   "bindParser
;;   Try `parser` and if it succeeds call f on the parsed value.
;;   Call the return of that with the remaining input. Otherwise return the error"
;;   [f parser]
;;   (fn [input]
;;     (let [{:keys [error value] :as result} (parser input)]
;;       (if error
;;         result
;;         (let [[parsed remaining-input] value
;;               parser2 (f parsed)]
;;           (parser2 remaining-input))))))

;; (defn returnParser [x]
;;   (fn [input] (->Either [x input] nil)))

;; (defn .>>.
;;   "andThen"
;;   [parser1 parser2]
;;   (>>= parser1 (fn [result1]
;;                  (>>= parser2
;;                       (fn [result2]
;;                         (returnParser [result1 result2]))))))

;; (defn choice [parsers]
;;   (reduce <|> parsers))

;; #_(defn anyOf [xs]
;;     (->> xs (map x-to-parser) choice))

;; ;; https://fsharpforfunandprofit.com/posts/understanding-parser-combinators-2/
;; ;; TODO rewrite to loop



;; (defn parseZeroOrMore [parser input]
;;   (let [{:keys [value error] :as result} (parser input)]
;;     (if error
;;       ['() input]
;;       (let [[parsed-first remaining-input] value
;;             [parsed-rest remaining-input] (parser remaining-input)]
;;         [(conj parsed-rest parsed-first) remaining-input]))))

;; ;; https://gist.github.com/swlaschin/a3dbb114a9ee95b2e30d#file-parserlibrary_v2-fsx

;; ;; class Monad
;; ;;   return :: a -> m a
;; ;;   bind (>>=) :: m a -> (a -> m b) -> m b



;; ;; parser: form -> (parsed, form)
;; (defn bind
;;   "bindParser
;;   Return a new parser that calls f on result of `parser`.
;;   Try `parser` and if it succeeds call f on the parsed value.
;;   Call the return of that with the remaining input. Otherwise return the error"
;;   [f parser]
;;   (fn [input]
;;     (let [{:keys [error value] :as result} (parser input)]
;;       (if error
;;         result
;;         (let [[parsed remaining-input] value
;;               parser2 (f parsed)]
;;           (parser2 remaining-input))))))

;; (def >>= bind)

;; (defn return [x]
;;   (fn [input] (->Either [x input] nil)))

;; (defn mapParser [f parser]
;;   (bind (return f) parser))

;; (def <!> mapParser)

;; (defn apply [fParser xParser]
;;   (>>= fParser (fn [f] (>>= xParser (fn [x] (return (f x)))))))

;; (def <*> apply)

;; (defn pull-parser [body]
;;   (match/match [body]
;;     [(['pull v [& pattern] & remaining] :seq)] (->Either [{:var v :pattern pattern} remaining] nil)
;;     :else (->Either nil "not a pull")))


;; (pull-parser '[pull ?a [foo] [bar baz [bam]]])


;; /// lift a two parameter function to Parser World
;; let lift2 f xP yP =
;;     returnP f <*> xP <*> yP

;; /// Combine two parsers as "A andThen B"
;; let andThen p1 p2 =
;;     p1 >>= (fun p1Result ->
;;     p2 >>= (fun p2Result ->
;;         returnP (p1Result,p2Result) ))

;; /// Infix version of andThen
;; let ( .>>. ) = andThen

;; /// Combine two parsers as "A orElse B"
;; let orElse p1 p2 =
;;     let innerFn input =
;;         // run parser1 with the input
;;         let result1 = run p1 input

;;         // test the result for Failure/Success
;;         match result1 with
;;         | Success result ->
;;             // if success, return the original result
;;             result1

;;         | Failure err ->
;;             // if failed, run parser2 with the input
;;             let result2 = run p2 input

;;             // return parser2's result
;;             result2

;;     // return the inner function
;;     Parser innerFn

;; /// Infix version of orElse
;; let ( <|> ) = orElse

;; /// Choose any of a list of parsers
;; let choice listOfParsers =
;;     List.reduce ( <|> ) listOfParsers

;; /// Choose any of a list of characters
;; let anyOf listOfChars =
;;     listOfChars
;;     |> List.map pchar // convert into parsers
;;     |> choice

;; /// Convert a list of Parsers into a Parser of a list
;; let rec sequence parserList =
;;     // define the "cons" function, which is a two parameter function
;;     let cons head tail = head::tail

;;     // lift it to Parser World
;;     let consP = lift2 cons

;;     // process the list of parsers recursively
;;     match parserList with
;;     | [] ->
;;         returnP []
;;     | head::tail ->
;;         consP head (sequence tail)

;; /// (helper) match zero or more occurences of the specified parser
;; let rec parseZeroOrMore parser input =
;;     // run parser with the input
;;     let firstResult = run parser input
;;     // test the result for Failure/Success
;;     match firstResult with
;;     | Failure err ->
;;         // if parse fails, return empty list
;;         ([],input)
;;     | Success (firstValue,inputAfterFirstParse) ->
;;         // if parse succeeds, call recursively
;;         // to get the subsequent values
;;         let (subsequentValues,remainingInput) =
;;             parseZeroOrMore parser inputAfterFirstParse
;;         let values = firstValue::subsequentValues
;;         (values,remainingInput)

;; /// matches zero or more occurences of the specified parser
;; let many parser =

;;     let rec innerFn input =
;;         // parse the input -- wrap in Success as it always succeeds
;;         Success (parseZeroOrMore parser input)

;;     Parser innerFn

;; /// matches one or more occurences of the specified parser
;; let many1 p =
;;     p      >>= (fun head ->
;;     many p >>= (fun tail ->
;;         returnP (head::tail) ))

;; /// Parses an optional occurrence of p and returns an option value.
;; let opt p =
;;     let some = p |>> Some
;;     let none = returnP None
;;     some <|> none

;; /// Keep only the result of the left side parser
;; let (.>>) p1 p2 =
;;     // create a pair
;;     p1 .>>. p2
;;     // then only keep the first value
;;     |> mapP (fun (a,b) -> a)

;; /// Keep only the result of the right side parser
;; let (>>.) p1 p2 =
;;     // create a pair
;;     p1 .>>. p2
;;     // then only keep the second value
;;     |> mapP (fun (a,b) -> b)

;; /// Keep only the result of the middle parser
;; let between p1 p2 p3 =
;;     p1 >>. p2 .>> p3

;; /// Parses one or more occurrences of p separated by sep
;; let sepBy1 p sep =
;;     let sepThenP = sep >>. p
;;     p .>>. many sepThenP
;;     |>> fun (p,pList) -> p::pList

;; /// Parses zero or more occurrences of p separated by sep
;; let sepBy p sep =
;;     sepBy1 p sep <|> returnP []



;;;;;;;;;;;;;;;;;;;;;; foobar

;; A parser is a function which accepts a string.
;; When the parsing is successful, the function returns a vector with two elements. The first element is the parsed value, and the second elemnt is the remaining string.
;;
;; On unsuccessful parsing, the return value is `nil`
;; Then type definition would look like this -
;; parser-fn :: <string> -> [<parsed-value>, <remaining-string>]


;; idk.. first i need to figure out of this is a good way of expressing the dsl
()
(def or-clause
  (p-seq 'or (p-plus clause)))

(def pattern-clause
  (p-length (p-seq any?) 3 or 4))

(defparser clause
  (or or-clause
      pattern-clause
      predicate-clause))



(defn satisfy [f]
  (fn [s]
    (let [fch (first s)]
      (if (and fch (f fch))
        [(str fch) (apply str (rest s))]))))

(defn compose-parsers [p1 p2]
  (fn [s]
    (if-let [[p1f rs1 :as p1-parsed] (p1 s)]
      (if-let [[p2f rs2 :as p2-parsed] (p2 rs1)]
        (cond (and (string? p1f) (string? p2f))
              [(str p1f p2f) rs2]

              (vector? p1f)
              [(vec (conj p1f p2f)) rs2]

              :else
              [[p1f p2f] rs2])))))

(defn compose-left [p1 p2]
  (fn [s]
    (if-let [[p1f rs1 :as p1-parsed] (p1 s)]
      (if-let [[p2f rs2 :as p2-parsed] (p2 rs1)]
        [p1f rs2]))))

(defn compose-right [p1 p2]
  (fn [s]
    (if-let [[p1f rs1 :as p1-parsed] (p1 s)]
      (if-let [[p2f rs2 :as p2-parsed] (p2 rs1)]
        [p2f rs2]))))

(defn compose-or [p1 p2]
  (fn [s]
    (if-let [[p-str, rem-str :as p1-parsed] (p1 s)]
      p1-parsed
      (p2 s))))

(defn compose-apply [p f]
  (fn [s]
    (if-let [[p-str rem-str] (p s)]
      [(f p-str) rem-str])))

(def <=> compose-parsers)
(def <* compose-left)
(def *> compose-right)
(def <|> compose-or)
(def <f> compose-apply)

(defn parse-char [ch]
  (satisfy #(= % ch)))

(defn parse-word [word]
  (reduce (fn [parser ch]
            (<=> parser (parse-char ch)))
          identity-parser word))

(def identity-parser
  (fn [s]
    ["" s]))

(defn optional-value [val]
  (fn [s]
    [val s]))

(defn optional-parser
  ([parser]
   (optional-parser parser ""))
  ([parser default-value]
   (fn [s]
     (if-let [result (parser s)]
       result
       [default-value s]))))

(declare one-or-more)

(defn zero-or-more [p]
  (<|> (one-or-more p)
       identity-parser))

(defn one-or-more [p]
  (fn [s]
    (let [[pf rs :as p-parsed] (p s)]
      (if p-parsed
        (if-let [[pf' rs' :as p-parsed'] ((zero-or-more p) rs)]
          [(str pf pf') rs']
          p-parsed)))))

(def digit
  (satisfy #(Character/isDigit %)))

(def alphanumeric
  (satisfy #(Character/isLetterOrDigit %)))

(def word
  (<* (one-or-more alphanumeric)
      (optional-parser (parse-char \ ))))

(def spaces (zero-or-more (parse-char \ )))

(defn safe-to-double [s]
  (if s
    (Double/parseDouble s)))

(defn safe-to-int [s]
  (if s
    (Integer/parseInt s)))

(def pos-int (one-or-more digit))

(def neg-int
  (-> (parse-char \-)
      (<=> pos-int)))

(def any-int
  (<|> neg-int pos-int))

(def pos-double
  (-> pos-int
      (<=> (parse-char \.))
      (<=> pos-int)))

(def neg-double
  (-> (parse-char \-)
      (<=> pos-double)))

(def pos-double
  (-> pos-int
      (<=> (parse-char \.))
      (<=> pos-int)))

(def any-double
  (<|> neg-double pos-double))

(def pos-number
  (<|> pos-double pos-int))

(def neg-number
  (<|> neg-double neg-int))

(def any-number
  (<|> neg-number pos-number))

(def parse-pos-int
  (<f> pos-int safe-to-int))

(def parse-neg-int
  (<f> neg-int safe-to-int))

(def parse-any-int
  (<f> any-int safe-to-int))

(def parse-pos-double
  (<f> pos-double safe-to-double))

(def parse-neg-double
  (<f> neg-double safe-to-double))

(def parse-pos-double
  (<f> pos-double safe-to-double))

(def parse-pos-number
  (<f> pos-number safe-to-double))

(def parse-neg-number
  (<f> neg-number safe-to-double))

(def parse-any-number
  (<f> any-number safe-to-double))
