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
  (sgetp [space templateFields])
  (sgetAll [space templateFields])
  (squery [space templateFields])
  (squeryp [space templateFields])
  (squeryAll [space templateFields]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; SequentialSpace impl;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(defn sputp- [space & fields] (-my-fn this args))
#_(def my-chan (async/chan 1))

;;trying to put more than one on the queue will block
;;until the queue is empty
(defn match-tupl? [col1 col2]
  (if (= (count col1) (count col2))
     (not (some false? (map #(match? %1 %2) col1 col2)))
    false))

(defn match-tupl2? [col1 col2]
  (if (= (count col1) (count col2))
    (not (some false? (map #(match? %1 (:v %2) ) col1 col2)))
    false))

(defn chan-size? [bound] (if (>= 0 bound) 1 bound))

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

(def x nil)

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
       (do

         (loop [x (try-get! space template)]
           (if (:return x)
               (do
                 (.notifyAll (:tuples!-lock space))
                 (:return x))
               (do
                 (.wait (:tuples!-lock space))
                 (recur (try-get! space template)))))


         #_(loop []
             (.wait (:get!-lock space))
             (when (not (:return (try-get! space template)))
               (recur)))

         #_(while (not (:return (try-get! space template)))
             (.wait (:get!-lock space)))
         #_(.notifyAll (:get!-lock space)))))))

(comment)

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


(def mySpace1 (new-SequentialSpace 1))

(def put-func
  (fn []
    (dotimes [x 5]
      ;;(.start
        ;;(Thread.
          ;;(future ;;(fn []
            (async/go        (do
                               (Thread/sleep (rand-int 10000))
                               (println (str "put-t:" x))
                               (try
                                 (put! mySpace1 (new-Template "hi" x))
                                 (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                                 (finally (println (str "put t:" x "done")))))))))

(def get-func
  (fn []
    (dotimes [x 5]
      ;;(.start
        ;;(Thread.
          ;;(future ;;(fn []
            (async/go        (do
                               (Thread/sleep (rand-int 10000))
                               (println (str "get-t:" x))
                               (try
                                 (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField x)))
                                 (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                                 (finally (println (str "get t:" x "done")))))))))




(comment
  (swap! mySec (fn [old new] (new-SequentialSpace- -1 [new])) [1 2])
  (ssize @mySec)

  (def mySec (new-SequentialSpace))
  (:bound mySec)

  (def mySec (new-SequentialSpace -5))
  (:bound mySec)

  (def mySec (new-SequentialSpace 6))
  (:bound mySec)

  #_(put! [space fields]
        (locking (:put!-lock space)
          (dosync
            (apply (fn [s f]
                     (let [tOld @(:tuples s)
                           tNew (alter (:tuples s)
                                       (fn [old new]
                                         (if (and (> (:bound s) 0) (>= (count old) (:bound s)))
                                           old
                                           (do
                                             (println old new)
                                             (conj old new))))
                                       f)]
                       (do
                         (if (not (= tOld tNew))
                           (do
                             (async/>!! put-chan "put-tuple-event")
                             true)
                           (let [tap-get-chan (async/chan (chan-size? bound))
                                 t- (async/tap (:mult-get-chan space) tap-get-chan)]
                             (do
                               (async/<!! tap-get-chan)
                               (recur @(:tuples s) f)))))))
                   [space fields]))))

  #_(dosync
      (apply (fn [s f]
               (let [tOld @(:tuples s)
                     tNew (alter (:tuples s)
                                 (fn [old new]
                                   (if (and (> (:bound s) 0) (>= (count old) (:bound s)))
                                     old
                                     (do
                                       (println old new)
                                       (conj old new))))
                                 f)]
                 (do
                   (if (not (= tOld tNew))
                     (do
                       (async/>!! put-chan "put-tuple-event")
                       true)
                     (let [tap-get-chan (async/chan (chan-size? (:bound space)))
                           t- (async/tap (:mult-get-chan space) tap-get-chan)]
                       (do
                         (async/<!! tap-get-chan)
                         (recur @(:tuples s) f)))))))
             [space fields])))












