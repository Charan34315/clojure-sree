(ns clojure-sree.fibonacci
  "Practice")

(defn recursive-fibonacci
  "Recursive implementation of the Fibonacci sequence."
  [n]
  (cond
    (= n 0) 0
    (= n 1) 1
    :else (+ (recursive-fibonacci (- n 1))
             (recursive-fibonacci (- n 2)))))

(map recursive-fibonacci [0 1 2 3 4 5 6 7 8 9])

(defn tail-recursive-fibonacci
  "Tail-recursive version of the fibonacci sequence.
   Tail recursion allows the runtime to reuse memory 
   locations for each subsequent call to our function. 
   This is more memory-efficient and won't crash if you give 
   it a bigger number for n"
  [n]
  (if (> n 1)
    (loop [x 1 fib0 0 fib1 1]
      (if (< x n)
        (recur (inc x) fib1 (+ fib0 fib1))
        fib1))
    n))

(map tail-recursive-fibonacci [0 1 2 3 4 5 6 7 8 9])

(def lazy-sequence-fibonacci
  "Create a lazy sequence version of the Fibonacci sequence.
   Create a sequence that provides the next element each time
   we access it."
  ((fn fib [a b] (lazy-seq (cons a (fib b (+ a b)))))
   0 1))

(take 15 lazy-sequence-fibonacci)

(def lazy-cat-fibonacci
  (lazy-cat [0 1]
            (map + (rest lazy-cat-fibonacci) lazy-cat-fibonacci)))

(take 10 lazy-cat-fibonacci)

(def seq-iterate-fibonacci
  (map first (iterate (fn [[a b]] [b (+ a b)]) [0 1])))

(take 10 seq-iterate-fibonacci)

; use of 'repeatedly' 
(defn counter []
  (let [tick (atom 100)]
    #(swap! tick inc)))

(def tick (counter))

(take 10 (repeatedly tick))








