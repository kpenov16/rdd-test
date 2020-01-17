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

(defrecord SequentialSpace [bound tuples ret-tupl get!-lock put-chan mult-put-chan]
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
          (do
            (async/>!! put-chan "new-tuple-event")
            true)))))

  (get! [space templateFields]
    (locking (:get!-lock space)
      (dosync
        (alter (:tuples space) (fn [tuples templ-fields]
                                 (let [tap-put-chan (async/chan (chan-size? bound))
                                       t- (async/tap (:mult-put-chan space) tap-put-chan)
                                       for_loop (for [t tuples]
                                                  (do
                                                    (println t templ-fields)
                                                    (match-it t templ-fields)))]


                                   (do
                                     #_(println (doall for_loop))
                                     (let [pos (.indexOf for_loop true)]
                                       (if (> pos -1)
                                           (do
                                             (ref-set (:ret-tupl space) (tuples pos))
                                             (vec (concat (subvec tuples 0 pos) (subvec tuples (inc pos)))))
                                           (do
                                             (async/<!! tap-put-chan)
                                             (recur (:tuples space) templ-fields))))
                                             ;;this recur might be recursive call to get! instead
                                     #_tuples))) templateFields)
        (deref (:ret-tupl space))))))


#_(if (= tNew (conj tOld fields))
    (do
      (put! put-chan "new-tuple-event")
      true)
    false)
(comment
  (def mySpace1 (new-SequentialSpace 2))
  (put! mySpace1 (new-Template 1 2))
  (put! mySpace1 (new-Template "hi" 2))
  (get! mySpace1 (new-Template (new-ActualField "hi") (new-ActualField 2))))


(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? @tuples)]}
   (let [ret-tupl (ref nil)
         get!-lock 0
         b (if (>= 0 bound) -1 bound)
         put-chan-size (chan-size? b)
         put-chan (async/chan put-chan-size)
         mult-put-chan (async/mult put-chan)]
     (->SequentialSpace b tuples ret-tupl get!-lock put-chan mult-put-chan))))

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











