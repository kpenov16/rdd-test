(ns rdd-test.core-test
  (:require [clojure.test :refer :all]
            [rdd-test.core :refer :all]))

;;;;;;;;;;;;;;;;;;;;;
;;FormalField tests;;
;;;;;;;;;;;;;;;;;;;;;
(deftest a-test
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