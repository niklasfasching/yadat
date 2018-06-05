(ns yadat.parser)

(defrecord Either [value error])

(->Either )

(defn any [input]
  (if-let [[first & rest] input]
    [first rest]
    []))

(any [])


let choice listOfParsers =
    List.reduce ( <|> ) listOfParsers


(defn <|> "orElse" [parser1 parser2]
  (fn [input]
    (let [result1 (parser1 input)]
      (if (:error result1)
        (parser2 input)
        result1))))

(defn >>= "bindParser" [f parser]
  (fn [input]
    (let [{:keys [error value] :as result} (parser input)]
      (if error
        result
        (let [[parsed remaining-input] value
              parser2 (f parsed)]
          (parser2 remaining-input))))))


/// Lift a value to a Parser
let returnP x =
    let innerFn input =
        // ignore the input and return x
        Success (x,input)
    // return the inner function
    Parser innerFn

(defn returnParser [x]
  (fn [input] (->Either [x input] nil)))

(defn .>>. "andThen" [parser1 parser2]
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
