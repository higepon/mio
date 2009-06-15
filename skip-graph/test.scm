(import (rnrs)
        (skip graph)
        (mosh test))

;; node
(let ([node (make-node "key1" "value1")])
  (test-equal "key1" (node-key node))
  (test-equal "value1" (node-value node))
  (test-eq 0 (node-membership node)))

;; node append!
(let ([node1 (make-node "key1" "value1")]
      [node2 (make-node "key2" "value2")])
  (node-append0! node1 node2)
  (test-eq 1 (node-membership node1))
  (test-eq 0 (node-membership node2))
  (test-eq node2 (node-next0 node1))
  (test-eq node1 (node-prev0 node2)))

;; skip graph.
(let ([sg (make-skip-graph)])
  (test-equal '() (skip-graph-level0-key->list sg))

  (skip-graph-add! sg (make-node 13 "$13"))
  (test-equal '(13) (skip-graph-level0-key->list sg))
  (test-equal '(() (13)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg (make-node 30 "$30"))
  (test-equal '(13 30) (skip-graph-level0-key->list sg))
  (test-equal '((30) (13)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg (make-node 20 "$20"))
  (test-equal '(13 20 30) (skip-graph-level0-key->list sg))
  (test-equal '((30) (13 20)) (skip-graph-level1-key->list sg))

  (skip-graph-add! sg (make-node 5 "$5"))
  (test-equal '(5 13 20 30) (skip-graph-level0-key->list sg))
  (test-equal '((5 30) (13 20)) (skip-graph-level1-key->list sg)))

(test-results)
