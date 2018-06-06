(ns yadat.parser
  (:require [clojure.core.match :as match]))

(defrecord Either [value error])

(defn any [input]
  (if-let [[first & rest] input]
    [first rest]
    []))

(any [])


let choice listOfParsers =
    List.reduce ( <|> ) listOfParsers


(defn <|>
  "orElse
  Try `parser1` and if it fails try `parser2`."
  [parser1 parser2]
  (fn [input]
    (let [result1 (parser1 input)]
      (if (:error result1)
        (parser2 input)
        result1))))

(defn >>=
  "bindParser
  Try `parser` and if it succeeds call f on the parsed value.
  Call the return of that with the remaining input. Otherwise return the error"
  [f parser]
  (fn [input]
    (let [{:keys [error value] :as result} (parser input)]
      (if error
        result
        (let [[parsed remaining-input] value
              parser2 (f parsed)]
          (parser2 remaining-input))))))

(defn returnParser [x]
  (fn [input] (->Either [x input] nil)))

(defn .>>.
  "andThen"
  [parser1 parser2]
  (>>= parser1 (fn [result1]
                 (>>= parser2
                      (fn [result2]
                        (returnParser [result1 result2]))))))

(defn choice [parsers]
  (reduce <|> parsers))



#_(defn anyOf [xs]
    (->> xs (map x-to-parser) choice))

;; https://fsharpforfunandprofit.com/posts/understanding-parser-combinators-2/
;; TODO rewrite to loop



(defn parseZeroOrMore [parser input]
  (let [{:keys [value error] :as result} (parser input)]
    (if error
      ['() input]
      (let [[parsed-first remaining-input] value
            [parsed-rest remaining-input] (parser remaining-input)]
        [(conj parsed-rest parsed-first) remaining-input]))))

;; https://gist.github.com/swlaschin/a3dbb114a9ee95b2e30d#file-parserlibrary_v2-fsx

;; class Monad
;;   return :: a -> m a
;;   bind (>>=) :: m a -> (a -> m b) -> m b



;; parser: form -> (parsed, form)
(defn bind
  "bindParser
  Return a new parser that calls f on result of `parser`.
  Try `parser` and if it succeeds call f on the parsed value.
  Call the return of that with the remaining input. Otherwise return the error"
  [f parser]
  (fn [input]
    (let [{:keys [error value] :as result} (parser input)]
      (if error
        result
        (let [[parsed remaining-input] value
              parser2 (f parsed)]
          (parser2 remaining-input))))))

(def >>= bind)

(defn return [x]
  (fn [input] (->Either [x input] nil)))

(defn mapParser [f parser]
  (bind (return f) parser))

(def <!> mapParser)

(defn apply [fParser xParser]
  (>>= fParser (fn [f] (>>= xParser (fn [x] (return (f x)))))))

(def <*> apply)


(defn pull-parser [body]
  (match/match [body]
    [(['pull v [& pattern] & remaining] :seq)] (->Either [{:var v :pattern pattern} remaining] nil)
    :else (->Either nil "not a pull")))

(pull-parser '[pull ?a [foo] [bar baz [bam]]])

/// lift a two parameter function to Parser World
let lift2 f xP yP =
    returnP f <*> xP <*> yP

/// Combine two parsers as "A andThen B"
let andThen p1 p2 =
    p1 >>= (fun p1Result ->
    p2 >>= (fun p2Result ->
        returnP (p1Result,p2Result) ))

/// Infix version of andThen
let ( .>>. ) = andThen

/// Combine two parsers as "A orElse B"
let orElse p1 p2 =
    let innerFn input =
        // run parser1 with the input
        let result1 = run p1 input

        // test the result for Failure/Success
        match result1 with
        | Success result ->
            // if success, return the original result
            result1

        | Failure err ->
            // if failed, run parser2 with the input
            let result2 = run p2 input

            // return parser2's result
            result2

    // return the inner function
    Parser innerFn

/// Infix version of orElse
let ( <|> ) = orElse

/// Choose any of a list of parsers
let choice listOfParsers =
    List.reduce ( <|> ) listOfParsers

/// Choose any of a list of characters
let anyOf listOfChars =
    listOfChars
    |> List.map pchar // convert into parsers
    |> choice

/// Convert a list of Parsers into a Parser of a list
let rec sequence parserList =
    // define the "cons" function, which is a two parameter function
    let cons head tail = head::tail

    // lift it to Parser World
    let consP = lift2 cons

    // process the list of parsers recursively
    match parserList with
    | [] ->
        returnP []
    | head::tail ->
        consP head (sequence tail)

/// (helper) match zero or more occurences of the specified parser
let rec parseZeroOrMore parser input =
    // run parser with the input
    let firstResult = run parser input
    // test the result for Failure/Success
    match firstResult with
    | Failure err ->
        // if parse fails, return empty list
        ([],input)
    | Success (firstValue,inputAfterFirstParse) ->
        // if parse succeeds, call recursively
        // to get the subsequent values
        let (subsequentValues,remainingInput) =
            parseZeroOrMore parser inputAfterFirstParse
        let values = firstValue::subsequentValues
        (values,remainingInput)

/// matches zero or more occurences of the specified parser
let many parser =

    let rec innerFn input =
        // parse the input -- wrap in Success as it always succeeds
        Success (parseZeroOrMore parser input)

    Parser innerFn

/// matches one or more occurences of the specified parser
let many1 p =
    p      >>= (fun head ->
    many p >>= (fun tail ->
        returnP (head::tail) ))

/// Parses an optional occurrence of p and returns an option value.
let opt p =
    let some = p |>> Some
    let none = returnP None
    some <|> none

/// Keep only the result of the left side parser
let (.>>) p1 p2 =
    // create a pair
    p1 .>>. p2
    // then only keep the first value
    |> mapP (fun (a,b) -> a)

/// Keep only the result of the right side parser
let (>>.) p1 p2 =
    // create a pair
    p1 .>>. p2
    // then only keep the second value
    |> mapP (fun (a,b) -> b)

/// Keep only the result of the middle parser
let between p1 p2 p3 =
    p1 >>. p2 .>> p3

/// Parses one or more occurrences of p separated by sep
let sepBy1 p sep =
    let sepThenP = sep >>. p
    p .>>. many sepThenP
    |>> fun (p,pList) -> p::pList

/// Parses zero or more occurrences of p separated by sep
let sepBy p sep =
    sepBy1 p sep <|> returnP []
