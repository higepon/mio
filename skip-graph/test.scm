(import (rnrs)
        (skip graph)
        (mosh test))

;; node
(let ([node (make-node "key1" "value1")])
  (test-equal "key1" (node-key node))
  (test-equal "value1" (node-value node))
  (test-eq 0 (node-membership node)))

;; node append!
(let ([level 0]
      [node1 (make-node "key1" "value1")]
      [node2 (make-node "key2" "value2")])
  (node-append! level node1 node2)
  (test-eq 1 (node-membership node1))
  (test-eq 0 (node-membership node2))
  (test-eq node2 (node-next level node1))
  (test-eq node1 (node-prev level node2)))

;; skip graph.
(let ([sg (make-skip-graph)]
      [node13 (make-node 13 "$13")]
      [node30 (make-node 30 "$30")]
      [node20 (make-node 20 "$20")]
      [node5 (make-node 5 "$5")]
      [node40 (make-node 40 "$40")]
      [node2 (make-node 2 "$2")])
  (test-equal '() (skip-graph-level0-key->list sg))

  (skip-graph-add! sg node13)
  (test-equal '(13) (skip-graph-level0-key->list sg))
  (test-equal '(() (13)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg node30)
  (test-equal '(13 30) (skip-graph-level0-key->list sg))
  (test-equal '((30) (13)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg node20)
  (test-equal '(13 20 30) (skip-graph-level0-key->list sg))
  (test-equal '((30) (13 20)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg node5)
  (test-equal '(5 13 20 30) (skip-graph-level0-key->list sg))
  (test-equal '((5 30) (13 20)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg node40)
  (test-equal '(5 13 20 30 40) (skip-graph-level0-key->list sg))
  (test-equal '((5 30) (13 20 40)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg node2)
  (test-equal '(2 5 13 20 30 40) (skip-graph-level0-key->list sg))
  (test-equal '((2 5 30) (13 20 40)) (skip-graph-level1-key->list sg))

  ;; start node is node30, search to left on level 1
  (let ([found (node-search node30 5)])
    (test-true found)
    (test-equal "$5" (node-value found)))

  ;; start node is node2, search to right on level 1
  (let ([found (node-search node2 5)])
    (test-true found)
    (test-equal "$5" (node-value found)))

  ;; start node is node20, search to left on level 0
  ;; これバグっている level1 しか探してない
  ;; prev list と next list が同じものかを見ないといけない。
  (let ([found (node-search node20 5)])
    (test-true found)
    (test-equal "$5" (node-value found)))

  )

(test-results)
