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


(defrecord SequentialSpace [bound tuples ret-tupl put-tupl get!-lock put!-lock put-chan mult-put-chan get-chan mult-get-chan]
  Space
  (ssize [space]
    (count (:tuples space)))

  (put! [space fields]
    ;;(dosync
    (locking (:put!-lock space)
     (do
       (println "put!: locked")
       (let [ps! (fn [fs] (dosync
                            (alter (:tuples space)
                                   (fn [old new]
                                     (let [b (:bound space)
                                           over-bound (and (> b 0) (>= (count old) b))]
                                       (if (true? over-bound)
                                         (do
                                           (println "put!: tuples not changed")
                                           (ref-set (:put-tupl space) -1)
                                           old)
                                         (do
                                           (println "put!: tuples changed" old new)
                                           (ref-set (:put-tupl space) new)
                                           (conj old new)))))
                                   fs)))]
         (do
           (println "put!: (ps! fields):" fields)
           (ps! fields)
           (while (and (= -1 (deref (:put-tupl space))) (number? (deref (:put-tupl space))))
             (let [tap-get-chan (async/chan (chan-size? (:bound space)))
                   t- (async/tap (:mult-get-chan space) tap-get-chan)]
               (do
                 (println "put!: tapping from get chan")
                 (println "put!: chan val:" (async/<!! tap-get-chan))
                 (println "put!: calling recur with " (:tuples space) fields)
                 (ps! fields))))
           (async/>!! put-chan "put-tuple-event")
           true))

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
                  [space fields])))))


  (get! [space templateFields]
    ;;(dosync
    (locking (:get!-lock space)
     (do
       (println "get!: locked by Thread:" (.getId (Thread/currentThread)))
       (let [ds! (fn [tfs]
                   (dosync
                     (alter (:tuples space)
                            (fn [tuples templ-fields]
                              (let [for_loop (for [t tuples]
                                               (do
                                                 (println t templ-fields)
                                                 (match-it t templ-fields)))]
                                (do
                                  (let [pos (.indexOf for_loop true)]
                                    (if (> pos -1)
                                        (do
                                          (println "get!: getting")
                                          (ref-set (:ret-tupl space) (tuples pos))
                                          (vec (concat (subvec tuples 0 pos) (subvec tuples (inc pos)))))
                                        (do
                                          (println "get!: setting -1")
                                          (ref-set (:ret-tupl space) -1)
                                          tuples))))))
                            tfs)))]
         (do
           (println "before ex:" templateFields)
           (ds! templateFields)
           (while (and (= -1 (deref (:ret-tupl space))) (number? (deref (:ret-tupl space))))
             (let [tap-put-chan (async/chan (chan-size? (:bound space)))
                   t- (async/tap (:mult-put-chan space) tap-put-chan)]
               (do
                 (println "get!: tapping from put chan")
                 (println "get!: chan val:" (async/<!! tap-put-chan))
                 (println "get!: calling recur with " (:tuples space) templateFields)
                 (ds! templateFields))))
           (async/>!! (:get-chan space) "get-tuple-event")
           (deref (:ret-tupl space))))))))
(comment
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
                     [space fields])))))

(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? @tuples)]}
   (let [ret-tupl (ref nil)
         put-tupl (ref nil)
         get!-lock (Object.)
         put!-lock (Object.)
         b (if (>= 0 bound) -1 bound)
         put-chan-size (chan-size? b)
         put-chan (async/chan put-chan-size)
         mult-put-chan (async/mult put-chan)
         get-chan-size put-chan-size
         get-chan (async/chan get-chan-size)
         mult-get-chan (async/mult get-chan)]
     (->SequentialSpace b tuples ret-tupl put-tupl get!-lock put!-lock put-chan mult-put-chan get-chan mult-get-chan))))

(defn new-SequentialSpace
  ([]
   (new-SequentialSpace -1))

  ([bound]
   {:pre [(int? bound)]}
   (new-SequentialSpace- (if (>= 0 bound) -1 bound) (ref []))))


(def mySpace1 (new-SequentialSpace 100))

(def put-func
  (fn []
    (dotimes [x 5]
      ;;(.start
        ;;(Thread.
          (future ;;(fn []
                    (do
                      (Thread/sleep (rand-int 10000))
                      (println "put-t:" x)
                      (try
                        (put! mySpace1 (new-Template "hi" x))
                        (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                        (finally (println "put t:" x "done"))))))))

(def get-func
  (fn []
    (dotimes [x 5]
      ;;(.start
        ;;(Thread.
          (future ;;(fn []
                    (do
                      (Thread/sleep (rand-int 10000))
                      (println "get-t:" x)
                      (try
                        (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField x)))
                        (catch Throwable t (println (str "t:" x "exception:" (.toString t))))
                        (finally (println "get t:" x "done"))))))))




(comment
  (swap! mySec (fn [old new] (new-SequentialSpace- -1 [new])) [1 2])
  (ssize @mySec)

  (def mySec (new-SequentialSpace))
  (:bound mySec)

  (def mySec (new-SequentialSpace -5))
  (:bound mySec)

  (def mySec (new-SequentialSpace 6))
  (:bound mySec))











