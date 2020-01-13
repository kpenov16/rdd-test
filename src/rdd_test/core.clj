(ns rdd-test.core)

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
  (sput [space fields])
  (sget [space templateFields])
  (sgetp [space templateFields])
  (sgetAll [space templateFields])
  (squery [space templateFields])
  (squeryp [space templateFields])
  (squeryAll [space templateFields]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; SequentialSpace impl;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(defn sputp- [space & fields] (-my-fn this args))

(defrecord SequentialSpace [bound tuples]
  Space
  (ssize [space]
    (count (:tuples space)))

  (sput [space fields]
    (locking space
      (dosync
        (let [tOld  @(:tuples space)
              tNew (alter (:tuples space)
                          (fn [old new]
                            (if (and (> (:bound space) 0) (>= (count old) (:bound space)))
                              old
                              (conj old new)))
                          fields)]
          (= tNew (conj tOld fields)))))))

(defn new-SequentialSpace-
  ([bound tuples]
   {:pre [(int? bound) (vector? @tuples)]}
   (->SequentialSpace (if (>= 0 bound) -1 bound) tuples)))

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











