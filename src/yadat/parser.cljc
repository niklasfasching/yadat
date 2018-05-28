(ns yadat.parser
  (:require [clojure.spec.alpha :as s]))

(s/def ::suit #{:club :diamond :heart :spade})
(s/conform ::suit :club)

(s/def ::big-even (s/and int? even? #(> % 1000)))
(s/conform ::big-even 10001)
(do
  (s/def :event/type keyword?)
  (s/def :event/timestamp int?)
  (s/def :search/url string?)
  (s/def :error/message string?)
  (s/def :error/code int?))

(defmulti event-type :event/type)
(defmethod event-type :event/search [_]
  (s/keys :req [:event/type :event/timestamp :search/url]))
(defmethod event-type :event/error [_]
  (s/keys :req [:event/type :event/timestamp :error/message :error/code]))
(s/def :event/event (s/multi-spec event-type :event/type))

(s/conform :event/event
           {:event/type :event/search
            :event/timestamp ""
            :search/url "200"})

(s/def ::vnum3 (s/coll-of number? :kind vector? :count 3 :distinct true :into #{}))
(s/conform ::vnum3 [1 2 3])
(s/assert ::vnum3 #{1 2 3})

(s/def ::foo (s/cat :x int? :y int? :z int?))
(s/conform ::foo [1 2 3])
;; especially the sequences one - allows nested
;; https://clojure.org/guides/spec
;; not sure if this makes sense... - does not unnest

(defn any [input]
  (if-let [c (first input)]
    (list [c (.substring input 1)])
    '()))

;; this one doesn't accept any input
(defn failure [_] '())

(any "clojure-1.9") ;; => ([\c lojure-1.9])


(defn parse [parser input]
  (parser input))

(defn parse-all [parser input]
  (->> input
       (parse parser)
       (filter #(= "" (second %)))
       (ffirst)))

;; builds parser that always returns given element without consuming (changing) input
(defn return [v]
  (fn [input] (list [v input])))

;; takes parser and function that builds new parsers from (each) result of applying first one
(defn >>= [m f]
  (fn [input]
    (->> input
         (parse m)
         (mapcat (fn [[v tail]] (parse (f v) tail)))))

  (defn merge-bind [body bind]
  (if (and (not= clojure.lang.Symbol (type bind))
           (= 3 (count bind))
           (= '<- (second bind)))
    `(>>= ~(last bind) (fn [~(first bind)] ~body))
    `(>>= ~bind (fn [~'_] ~body))))

(defmacro do* [& forms]
  (reduce merge-bind (last forms) (reverse (butlast forms)))))

(defn- -sat [pred]
  (>>= any (fn [v] (if (pred v) (return v) failure))))

;; just a helper
(defn- -char-cmp [f]
  (fn [c] (-sat (partial f (first c)))))

;; recognizes given char
(def match (-char-cmp =))

;; rejects given char
(def none-of (-char-cmp not=))

;; just a helper
(defn- -from-re [re]
  (-sat (fn [v] (not (nil? (re-find re (str v)))))))

;; recognizes any digit
(def digit (-from-re #"[0-9]"))

;; recognizes any letter
(def letter (-from-re #"[a-zA-Z]"))

;; (ab)
(defn and-then [p1 p2]
  (do*
   (r1 <- p1)
   (r2 <- p2)
   ;; xxx: note, that it's dirty hack to use STR to concat outputs,
   ;; full functional implementation should use MonadPlus protocol
   (return (str r1 r2))))

;; (a|b)
(defn or-else [p1 p2]
  (fn [input]
    (lazy-cat (parse p1 input) (parse p2 input))))

(declare plus)
(declare optional)

;; (a*)
(defn many [parser]
  (optional (plus parser)))

;; (a+) equals to (aa*)
(defn plus [parser]
  (do*
   (a <- parser)
   (as <- (many parser))
   (return (cons a as))))

;; (a?)
(defn optional [parser]
  (or-else parser (return "")))

(defn plus [parser]
  (>>= parser
       (fn [a] (>>= (many parser)
                    (fn [as] (return (cons a as)))))))


;; recognizes space (or newline)
(def space (or-else (match " ") (match "\n")))

;; recognizes empty string or arbitrary number of spaces
(def spaces (many space))

;; recognizes given string, i.e. "clojure"
(defn string [s]
  (reduce and-then (map #(match (str %)) s)))


(def clojure-version (do*
                      (string "clojure")
                      (match " ")
                      (major <- digit)
                      (match ".")
                      (minor <- digit)
                      (return (str "major: " major "; minor: " minor))))

(parse-all clojure-version "clojure 1.7")
