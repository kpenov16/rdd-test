(ns rdd-test.core
  (:require [clojure.core.async :as async]))

(defprotocol TemplateField
  (match? [field o]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; FormalField impl ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord FormalField [v]
  TemplateField
  (match? [field o]
    (cond
      (or (nil? o) (nil? (:v field))) false
      :else (instance? (type o) (:v field)))))

(defn new-FormalField [v]
  (if (nil? v)
    (throw (NullPointerException. "The value passed to FormalField cannot be nil"))
    (->FormalField v)))

(comment
  (def myFormalField (new-FormalField 5))
  (match? myFormalField 5))

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ActualField impl ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord ActualField [v]
  TemplateField
  (match? [field o]
    (cond
      (or (nil? o) (nil? (:v field))) false
      :else (and (instance? (type o) (:v field)) (= o (:v field))))))

(defn new-ActualField [v]
  (if (nil? v)
    (throw (NullPointerException. "The value passed to ActualField cannot be nil"))
    (->ActualField v)))

(comment
  (def myActualField (new-ActualField 5))
  (match? myActualField 5))

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Template impl ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol ITemplate
  (match-it [thisTemplate otherTemplate]))

(defrecord Template [templateFields]
  ITemplate
  (match-it [thisTemplate otherTemplate]
    (if (= (count (:templateFields thisTemplate)) (count (:templateFields otherTemplate)))
      (not (some false? (map #(match? %1 (:v %2) ) (:templateFields otherTemplate) (:templateFields thisTemplate))))
      false)))

(defn new-Template [& args]
  (if (nil? args)
    (throw (NullPointerException. "The value passed to Template cannot be nil"))
    (let [fields (if (vector? args) args (vec args))]
      (->Template
        (into [] (for [f fields]
                   (if (satisfies? TemplateField f) f (new-ActualField f))))))))


(comment
  (def myTemplate (new-Template 2 5))
  (def myTemplate (new-Template 5))
  (def myTemplate (new-Template (new-FormalField 5) (new-FormalField "hi"))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tuple impl ;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

#_(defrecord Tuple [elements]

    (length [this] (count (:elements this)))
    (get-element-at [this i] ((:elements this) i)))

#_(defn new-Tuple [elements]
    (->Tuple elements))

(comment)

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Space ;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Space
  (ssize [space])
  (put! [space fields])
  (get! [space templateFields])
  (getp! [space templateFields])
  (getAll! [space templateFields])
  (query [space templateFields])
  (query-try! [space templateFields])
  (queryp [space templateFields])
  (queryAll [space templateFields]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; SequentialSpace impl;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord SequentialSpace [bound tuples tuples!-lock]
  Space
  (ssize [space]
    (count (:value (:tuples space))))

  (put! [space fields]
    (locking (:tuples!-lock space)
     (do
       (while (true? (let [b (:bound space)
                           over-bound (and (> b 0) (>= (count (:value @(:tuples space))) b))]
                       over-bound))
         (.wait (:tuples!-lock space)))
       (swap! (:tuples space)
              (fn [old new]
                (if (and (> (:bound space) 0) (>= (count (:value old)) (:bound space)))
                    old
                    {:value (conj (:value old) new)
                     :return (:return old)}))
              fields)
       (.notifyAll (:tuples!-lock space))
       true)))

  (get! [space template]
    (locking (:tuples!-lock space)
     (let [try-get! (fn [space template]
                      (swap! (:tuples space)
                             (fn [tuples template]
                               (let [for_loop (for [t (:value tuples)] (match-it t template))
                                     pos (.indexOf for_loop true)]
                                 (if (> pos -1)
                                   {:value (vec (concat (subvec (:value tuples) 0 pos) (subvec (:value tuples) (inc pos))))
                                    :return ((:value tuples) pos)}
                                   {:value (:value tuples)
                                    :return false})))
                             template))]
       (loop [x (try-get! space template)]
         (if (:return x)
             (do
               (.notifyAll (:tuples!-lock space))
               (:return x))
             (do
               (.wait (:tuples!-lock space))
               (recur (try-get! space template))))))))

  (getp! [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [space template]
                       (swap! (:tuples space)
                              (fn [tuples template]
                                (let [for_loop (for [t (:value tuples)] (match-it t template))
                                      pos (.indexOf for_loop true)]
                                  (if (> pos -1)
                                    {:value (vec (concat (subvec (:value tuples) 0 pos) (subvec (:value tuples) (inc pos))))
                                     :return ((:value tuples) pos)}
                                    {:value (:value tuples)
                                     :return false})))
                              template))]
        (let [x (try-get! space template)]
          (if (:return x)
             (do
              (.notifyAll (:tuples!-lock space))
              (:return x))
             nil)))))

  (query-try! [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [space template]
                       (swap! (:tuples space)
                              (fn [tuples template]
                                (let [for_loop (for [t (:value tuples)] (match-it t template))
                                      pos (.indexOf for_loop true)]
                                  (if (> pos -1)
                                    {:value (:value tuples)
                                     :return ((:value tuples) pos)}
                                    {:value (:value tuples)
                                     :return false})))
                              template))]
        (loop [x (try-get! space template)]
          (if (:return x)
             (:return x)
             (do
              (.wait (:tuples!-lock space))
              (recur (try-get! space template))))))))

  (query [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [tuples template]
                       (let [for_loop (for [t (:value tuples)] (match-it t template))
                             pos (.indexOf for_loop true)]
                         (if (> pos -1)
                           {:value "don't care"
                            :return ((:value tuples) pos)}
                           {:value "don't care"
                            :return false})))]
        (loop [x (try-get! @(:tuples space) template)]
          (if (:return x)
            (:return x)
            (do
             (.wait (:tuples!-lock space))
             (recur (try-get! @(:tuples space) template))))))))

  (queryp [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [tuples template]
                       (let [for_loop (for [t (:value tuples)] (match-it t template))
                             pos (.indexOf for_loop true)]
                         (if (> pos -1)
                           {:value "don't care"
                            :return ((:value tuples) pos)}
                           {:value "don't care"
                            :return false})))]
        (loop [x (try-get! @(:tuples space) template)]
          (if (:return x)
            (:return x)
            nil)))))

  (queryAll [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [tuples template]
                       (let [vals-indexed (map-indexed vector (:value tuples))
                             all-matched (map second (filter #(match-it (second %) template) vals-indexed))]
                         (if (not (empty? all-matched))
                           {:value "don't care"
                            :return (vec all-matched)}
                           {:value "don't care"
                            :return false})))]
        (let [x (try-get! @(:tuples space) template)]
          (if (:return x)
            (:return x)
            nil)))))

  (getAll! [space template]
    (locking (:tuples!-lock space)
      (let [try-get! (fn [space template]
                       (swap! (:tuples space)
                              (fn [tuples template]
                                (let [vals-indexed (map-indexed vector (:value tuples))
                                      all-matched (map second (filter #(match-it (second %) template) vals-indexed))
                                      all-not-matched (map second (filter #(not (match-it (second %) template)) vals-indexed))]
                                  (if (not (empty? all-matched))
                                    {:value (vec all-not-matched)
                                     :return (vec all-matched)}
                                    {:value (:value tuples)
                                     :return false})))
                              template))]
        (let [x (try-get! space template)]
          (if (:return x)
            (do
             (.notifyAll (:tuples!-lock space))
             (:return x))
            nil))))))

(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? (:value @tuples))]}
   (let [tuples!-lock (Object.)
         b (if (>= 0 bound) -1 bound)]
     (->SequentialSpace b tuples tuples!-lock))))

(defn new-SequentialSpace
  ([]
   (new-SequentialSpace -1))

  ([bound]
   {:pre [(int? bound)]}
   (new-SequentialSpace- (if (>= 0 bound) -1 bound) (atom {:value []
                                                           :return nil}))))

(def mySpace1 (new-SequentialSpace 5))

(def put-func
  (fn []
    (dotimes [x 5]
      (.start
        (Thread.
          ;;(future
          (fn []
            ;;(async/go
              (do
                (Thread/sleep (rand-int 10000))
                (println (str "put-t:" x))
                (try
                  (println (str "put-t:" x "returned:")
                           (put! mySpace1 (new-Template "hi" x)))
                  (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                  (finally (println (str "put t:" x "done")))))))))))

(def get-func
  (fn []
    (dotimes [x 5]
      (.start
        (Thread.
          ;;(future
          (fn []
            (async/go
              (do
                (Thread/sleep (rand-int 10000))
                (println (str "get-t:" x))
                (try
                  (println (str "get-t:" x "returned:")
                           (:templateFields (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField x)))))
                  (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                  (finally (println (str "get t:" x "done"))))))))))))

(def putDuplicates-func
  (fn []
    (dotimes [n 3]
      (dotimes [x 5]
        (.start
          (Thread.
            ;;(future
            (fn []
              ;;(async/go
              (do
                (Thread/sleep (rand-int 10000))
                (println (str "put-t:" n x))
                (try
                  (println (str "put-t:" n x "returned:")
                           (put! mySpace1 (new-Template "hi" x)))
                  (catch Throwable t (println (str "t:" n x "exception:" (.toString t))))
                  (finally (println (str "put t:" n x "done"))))))))))))

(def getAll-func
  (fn []
    (dotimes [x 5]
      (.start
        (Thread.
          ;;(future
          (fn []
            (async/go
              (do
                (Thread/sleep (rand-int 10000))
                (println (str "get-t:" x))
                (try
                  (println (str "get-t:" x "returned:")
                           (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField x))))
                  (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                  (finally (println (str "get t:" x "done"))))))))))))

;; create SequentialSpace with bound 1 and block on second call to put!
(comment
  (def mySpace1 (new-SequentialSpace 1))
  (put! mySpace1 (new-Template 1 2))
  ;;block after second put!
  (put! mySpace1 (new-Template "hi" 2)))

(defmacro do-while
  [test & body]
  `(loop []
     ~@body
     (when ~test
       (recur))))

(comment
  (swap! mySec (fn [old new] (new-SequentialSpace- -1 [new])) [1 2])
  (ssize @mySec)

  (def mySec (new-SequentialSpace))
  (:bound mySec)

  (def mySec (new-SequentialSpace -5))
  (:bound mySec)

  (def mySec (new-SequentialSpace 6))
  (:bound mySec))