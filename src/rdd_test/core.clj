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


(defrecord SequentialSpace [bound tuples ret-tupl get!-lock put!-lock put-chan mult-put-chan get-chan mult-get-chan]
  Space
  (ssize [space]
    (count (:tuples space)))

  (put! [space fields]
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
               [space fields])


        #_(do
            (async/>!! (:put-chan space) "new-tuple-event")
            (println "el in buff" (.count (.buf put-chan)))
            (println "size of buff" (.n (.buf put-chan)))
            true))))


  (get! [space templateFields]
    (locking (:get!-lock space)
      (let [ds! (fn [tfs]
                  (dosync
                    (alter (:tuples space)
                           (fn [tuples templ-fields]
                             (let [;;tap-put-chan (async/chan (chan-size? (:bound space)))
                                   ;;t- (async/tap (:mult-put-chan space) tap-put-chan)
                                   for_loop (for [t tuples]
                                              (do
                                                (println t templ-fields)
                                                (match-it t templ-fields)))]
                               (do
                                 #_(println (doall for_loop))
                                 (let [pos (.indexOf for_loop true)]
                                   (if (> pos -1)
                                       (do
                                         (async/>!! (:get-chan space) "get-tuple-event")
                                         (ref-set (:ret-tupl space) (tuples pos))
                                         (vec (concat (subvec tuples 0 pos) (subvec tuples (inc pos)))))
                                       (do
                                         (ref-set (:ret-tupl space) -1)
                                         tuples))))))
                           tfs)))]
        (do
          (ds! templateFields)
          (while (= -1 (deref (:ret-tupl space)))
            (do
              (let [tap-put-chan (async/chan (chan-size? (:bound space)))
                    t- (async/tap (:mult-put-chan space) tap-put-chan)]
                (do
                  (println "tapping from put chan")
                  (println "chan val:" (async/<!! tap-put-chan))
                  (println "calling recur with " (:tuples space) templateFields)
                  (ds! templateFields)
                  #_(recur (:tuples space) templ-fields)))))
          (deref (:ret-tupl space)))))))





(comment
  (def mySpace1 (new-SequentialSpace 2))
  (put! mySpace1 (new-Template 1 2))
  (put! mySpace1 (new-Template "hi" 2))
  (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField 2))))


(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? @tuples)]}
   (let [ret-tupl (ref nil)
         get!-lock (Object.)
         put!-lock (Object.)
         b (if (>= 0 bound) -1 bound)
         put-chan-size (chan-size? b)
         put-chan (async/chan put-chan-size)
         mult-put-chan (async/mult put-chan)
         get-chan-size put-chan-size
         get-chan (async/chan get-chan-size)
         mult-get-chan (async/mult get-chan)]
     (->SequentialSpace b tuples ret-tupl get!-lock put!-lock put-chan mult-put-chan get-chan mult-get-chan))))

(defn new-SequentialSpace
  ([]
   (new-SequentialSpace -1))

  ([bound]
   {:pre [(int? bound)]}
   (new-SequentialSpace- (if (>= 0 bound) -1 bound) (ref []))))



(comment
  (swap! mySec (fn [old new] (new-SequentialSpace- -1 [new])) [1 2])
  (ssize @mySec)

  (def mySec (new-SequentialSpace))
  (:bound mySec)

  (def mySec (new-SequentialSpace -5))
  (:bound mySec)

  (def mySec (new-SequentialSpace 6))
  (:bound mySec))











