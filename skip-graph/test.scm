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
      [node2 (make-node 2 "$2")]
      [node6 (make-node 6 "$6")])
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

  (skip-graph-add! sg node6)
  (test-equal '(2 5 6 13 20 30 40) (skip-graph-level0-key->list sg))
  (test-equal '((2 5 30) (6 13 20 40)) (skip-graph-level1-key->list sg))

  ;; start node is node30, search to left on level 1
  (let-values (([found path] (node-search node30 5)))
    (test-true found)
    (test-equal '((1 . 30) (1 . 5) found) path)
    (test-equal "$5" (node-value found)))

  ;; start node is node2, search to right on level 1
  (let-values (([found path] (node-search node2 5)))
    (test-true found)
    (test-equal '((1 . 2) (1 . 5) found) path)
    (test-equal "$5" (node-value found)))

  ;; start node is node20, search to left on level 0
  (let-values (([found path] (node-search node20 5)))
    (test-true found)
    (test-equal '((1 . 20) (1 . 13) (1 . 6) (0 . 6) (0 . 5) found) path)
    (test-equal "$5" (node-value found)))

  ;; start node is node40, search to left on level 0
  (let-values (([found path] (node-search node40 5)))
    (test-true found)
    (test-equal '((1 . 40) (1 . 20) (1 . 13) (1 . 6) (0 . 6)  (0 . 5) found) path)
    (test-equal "$5" (node-value found)))

  (let-values (([found path] (node-search node2 40)))
    (test-true found)
    (test-equal '((1 . 2) (1 . 5) (1 . 30) (0 . 30) (0 . 40) found) path)
    (test-equal "$40" (node-value found)))

  ;; not found
  (let-values (([found path] (node-search node40 4)))
    (test-equal '((1 . 40) (1 . 20) (1 . 13) (1 . 6) (0 . 6) (0 . 5)) path)
    (test-false found))

  ;; not found
  (let-values (([found path] (node-search node40 1000)))
    (test-equal '((1 . 40) (0 . 40)) path)
    (test-false found))

  ;; range search
  (let-values (([found path] (node-range-search node40 13 25)))
    (test-equal '((13 . "$13") (20 . "$20")) (map (lambda (node) (cons (node-key node) (node-value node))) found)))

  (let-values (([found path] (node-range-search node2 13 25)))
    (test-equal '((13 . "$13") (20 . "$20")) (map (lambda (node) (cons (node-key node) (node-value node))) found)))

(test-results))
