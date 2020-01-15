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
;; Template impl ;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol ITemplate
  (match-it [thisTemplate otherTemplate]))

(defrecord Template [templateFields]
  ITemplate
  (match-it [thisTemplate otherTemplate]
    (if (= (count (:templateFields thisTemplate)) (count (:templateFields otherTemplate)))
      (not (some false? (map #(match? %1 (:v %2) ) (:templateFields thisTemplate) (:templateFields otherTemplate))))
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



(defrecord SequentialSpace [bound tuples put-chan get-chan]
  Space
  (ssize [space]
    (count (:tuples space)))

  (put! [space fields]
    (locking space
      (dosync
        (let [tOld  @(:tuples space)
              tNew (alter (:tuples space)
                          (fn [old new]
                            (if (and (> (:bound space) 0) (>= (count old) (:bound space)))
                              old
                              (conj old new)))
                          fields)]
          (if (= tNew (conj tOld fields))
            (do
              (put! put-chan "new-tuple-event")
              true)
            false)))))

  (get! [space templateFields]
    (locking space
      (dosync
        (alter (:tuples space) (fn [tuples templ-fields]
                                 (for [fields tuples]
                                   (for [field fields
                                         to-find-field templ-fields]
                                     (if (match? to-find-field field)
                                       true
                                       false))) templateFields))))))



(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? @tuples)]}
   (let [b (if (>= 0 bound) -1 bound)
         put-chan-size (if (= b -1) 1 b)
         get-chan-size (if (= b -1) 1 b)]
     (->SequentialSpace b tuples (async/chan put-chan-size) (async/chan get-chan-size)))))

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











