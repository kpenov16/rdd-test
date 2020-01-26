(ns rdd-test.core-test
  (:require [clojure.test :refer :all]
            [rdd-test.core :refer :all]))

;;;;;;;;;;;;;;;;;;;;;
;;ActualField tests;;
;;;;;;;;;;;;;;;;;;;;;
(deftest ActualField-tests
  (testing "givenNewActualFieldCreatedWithNilValue_returnNullPointerException"
    (is (thrown? NullPointerException (new-ActualField nil))))

  (testing "givenNewActualFieldWithStringValue_returnStringValueInActualField"
    (let [stringValue "stringValue"
          emptyString ""]
      (is (= stringValue (:v (new-ActualField stringValue))))
      (is (= emptyString (:v (new-ActualField emptyString))))))


  (testing "givenNewActualFieldWithIntValue_returnIntValueInActualField"
    (let [negativeInt -42
          zeroInt 0
          positiveInt 42]
      (is (= negativeInt (:v (new-ActualField negativeInt))))
      (is (= zeroInt (:v (new-ActualField zeroInt))))
      (is (= positiveInt (:v (new-ActualField positiveInt))))))

  (testing "givenNewActualFieldWithVector_returnVectorInActualField"
    (is (= [1 2 :s "p"] (:v (new-ActualField [1 2 :s "p"])))))

  (testing "givenNewActualFieldWithMap_returnMapInActualField"
    (is (= {:p 42 42 :p "pi" 3.14 3.14 "pi"} (:v (new-ActualField {:p 42 42 :p "pi" 3.14 3.14 "pi"})))))

  (testing "givenNewActualFieldWithSet_returnSetInActualField"
    (is (= #{:p 42 "pi" 3.14} (:v (new-ActualField #{:p 42 "pi" 3.14})))))

  (testing "givenNewActualFieldWithSymbolValue_returnSymbolValueInActualField"
    (is (= :someSymbol (:v (new-ActualField :someSymbol)))))

  (testing "givenSymbolMatchingTheValueInActualField_returnTrue"
    (let [symbol :s
          actualField (new-ActualField symbol)]
      (is (match? actualField symbol))))

  (testing "givenSymbolNotMatchingTheValueInActualField_returnFalse"
    (let [symbol :s
          actualField (new-ActualField symbol)]
      (is (not (match? actualField :otherSymbol)))))

  (testing "givenNumberMatchedToSymbolValueInActualField_returnFalse"
    (let [symbol :s
          actualField (new-ActualField symbol)]
      (is (not (match? actualField 42)))))

  (testing "givenStringMatchedToSymbolValueInActualField_returnFalse"
    (let [symbol :s
          actualField (new-ActualField symbol)]
      (is (not (match? actualField "42")))))

  (testing "givenNilMatchedToSymbolValueInActualField_returnFalse"
    (let [symbol :s
          actualField (new-ActualField symbol)]
      (is (not (match? actualField nil)))))

  (testing "givenSymbolMatchedToNumberValueInActualField_returnFalse"
    (let [actualField (new-ActualField 42)]
      (is (not (match? actualField :symbol)))))

  (testing "givenStringMatchedToNumberValueInActualField_returnFalse"
    (let [actualField (new-ActualField 42)]
      (is (not (match? actualField "some_string")))))

  (testing "givenNilMatchedToNumberValueInActualField_returnFalse"
    (let [actualField (new-ActualField 42)]
      (is (not (match? actualField nil)))))

  (testing "givenSymbolMatchedToStringValueInActualField_returnFalse"
    (let [actualField (new-ActualField "some_string")]
      (is (not (match? actualField :symbol)))))

  (testing "givenNumberMatchedToStringValueInActualField_returnFalse"
    (let [actualField (new-ActualField "some_string")]
      (is (not (match? actualField 42)))))

  (testing "givenNilMatchedToStringValueInActualField_returnFalse"
    (let [actualField (new-ActualField "some_string")]
      (is (not (match? actualField nil))))))






;;;;;;;;;;;;;;;;;;;;;
;;FormalField tests;;
;;;;;;;;;;;;;;;;;;;;;
(deftest FormalField-tests
  (testing "givenNewFormalFieldCreatedWithNilValue_returnNullPointerException"
    (is (thrown? NullPointerException (new-FormalField nil))))

  (testing "givenNewFormalFieldWithString_returnStringInFormalField"
    (is (= "value" (:v (new-FormalField "value")))))

  (testing "givenNewFormalFieldWithInt_returnIntInFormalField"
    (is (= 1 (:v (new-FormalField 1)))))

  (testing "givenNewFormalFieldWithSymbol_returnSymbolInFormalField"
    (is (= :s (:v (new-FormalField :s)))))

  (testing "givenSymbolMatchingTheValueInFormalField_returnTrue"
    (let [symbol :s
          formalField (new-FormalField symbol)]
      (is (match? formalField symbol))))

  (testing "givenSymbolNotMatchingTheValueInFormalField_returnStillTrue"
    (let [symbol :s
          formalField (new-FormalField symbol)]
      (is (match? formalField :otherSymbol))))

  (testing "givenNumberMatchedToSymbolValueInFormalField_returnFalse"
    (let [symbol :s
          formalField (new-FormalField symbol)]
      (is (not (match? formalField 42)))))

  (testing "givenStringMatchedToSymbolValueInFormalField_returnFalse"
    (let [symbol :s
          formalField (new-FormalField symbol)]
      (is (not (match? formalField "42")))))

  (testing "givenNilMatchedToSymbolValueInFormalField_returnFalse"
    (let [symbol :s
          formalField (new-FormalField symbol)]
      (is (not (match? formalField nil)))))

  (testing "givenSymbolMatchedToNumberValueInFormalField_returnFalse"
    (let [formalField (new-FormalField 42)]
      (is (not (match? formalField :symbol)))))

  (testing "givenStringMatchedToNumberValueInFormalField_returnFalse"
    (let [formalField (new-FormalField 42)]
      (is (not (match? formalField "some_string")))))

  (testing "givenNilMatchedToNumberValueInFormalField_returnFalse"
    (let [formalField (new-FormalField 42)]
      (is (not (match? formalField nil)))))

  (testing "givenSymbolMatchedToStringValueInFormalField_returnFalse"
    (let [formalField (new-FormalField "some_string")]
      (is (not (match? formalField :symbol)))))

  (testing "givenNumberMatchedToStringValueInFormalField_returnFalse"
    (let [formalField (new-FormalField "some_string")]
      (is (not (match? formalField 42)))))

  (testing "givenNilMatchedToStringValueInFormalField_returnFalse"
    (let [formalField (new-FormalField "some_string")]
      (is (not (match? formalField nil))))))


(run-tests)