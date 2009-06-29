(import (rnrs)
        (skip graph)
        (srfi :39)
        (srfi :8)
        (mosh test))

;; link-op, buddy-op
(parameterize ([max-level 1]
               [membership-counter 0])
(let ([node13 (make-node 13 "$13")]
      [node30 (make-node 30 "$30")]
      [node20 (make-node 20 "$20")]
      [node5 (make-node 5 "$5")]
      [node40 (make-node 40 "$40")]
      [node2 (make-node 2 "$2")]
      [node6 (make-node 6 "$6")]
      [node9 (make-node 9 "$9")])

  (link-op node13 node20 'RIGHT 0)
  (test-equal '((13 20)) (node->key-list 0 node13))
  (test-equal '((13) (20)) (node->key-list 1 node13))

  (let ([found (buddy-op node13 node13 node40 1 'dummy 'RIGHT)])
    (test-true found)
    (test-eq 13 (node-key found)))

  (link-op node20 node40 'RIGHT 0)
  (test-equal '((13 20 40)) (node->key-list 0 node13))

))

;skip graph.
(parameterize ([max-level 1]
               [membership-counter 0])
(let ([node13 (make-node 13 "$13")]
      [node30 (make-node 30 "$30")]
      [node20 (make-node 20 "$20")]
      [node5 (make-node 5 "$5")]
      [node40 (make-node 40 "$40")]
      [node2 (make-node 2 "$2")]
      [node6 (make-node 6 "$6")]
      [node9 (make-node 9 "$9")])

  (node-insert! node13 node13)
  (test-equal '((13)) (node->key-list 0 node13))
  (test-equal '((13)) (node->key-list 1 node13))

  (node-insert! node13 node30)
  (test-equal '((13 30)) (node->key-list 0 node13))
  (test-equal '((13) (30)) (node->key-list 1 node13))

  (node-insert! node30 node20)
  (test-equal '((13 20 30)) (node->key-list 0 node30))
  (test-equal '((13 20) (30)) (node->key-list 1 node13))

  (node-insert! node30 node5)
  (test-equal '((5 13 20 30)) (node->key-list 0 node20))
  (test-equal '((13 20) (5 30)) (node->key-list 1 node20))

  (node-insert! node30 node40)
  (test-equal '((5 13 20 30 40)) (node->key-list 0 node5))
  (test-equal '((13 20 40) (5 30) ) (node->key-list 1 node20))

  (node-insert! node30 node2)
  (test-equal '((2 5 13 20 30 40)) (node->key-list 0 node5))
  (test-equal '((13 20 40) (2 5 30)) (node->key-list 1 node5))

  (node-insert! node13 node6)
  (test-equal '((2 5 6 13 20 30 40)) (node->key-list 0 node5))
  (test-equal '((6 13 20 40) (2 5 30) ) (node->key-list 1 node2))

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
    (test-equal '((13 . "$13") (20 . "$20")) found))

  (let-values (([found path] (node-range-search node2 13 25)))
    (test-equal '((13 . "$13") (20 . "$20")) found))

  ;; range search
  (let-values (([found path] (node-range-search node40 13 25 1)))
    (test-equal '((13 . "$13")) found))

  (let ([level 0])
    (node-insert! node30 node9)
    (test-equal '((2 5 6 9 13 20 30 40)) (node->key-list 0 node30))
    (test-equal '((6 13 20 40) (2 5 9 30)) (node->key-list 1 node9)))))

;; level0, level1 and leve2
(parameterize ([max-level 2]
               [membership-counter 0])
  (let ([node13 (make-node 13 "$13")]
        [node2 (make-node 2 "$2")]
        [node9 (make-node 9 "$9")]
        [node40 (make-node 40 "$40")]
        [node5 (make-node 5 "$5")]
        )

    (test-equal '((13)) (node->key-list 0 node13))
    (test-equal '((13)) (node->key-list 1 node13))
    (test-equal '((13)) (node->key-list 2 node13))

    (node-insert! node13 node2)
    (test-equal '((2 13)) (node->key-list 0 node13))
    (test-equal '((13) (2)) (node->key-list 1 node13))
    (test-equal '((13) (2)) (node->key-list 2 node13))

    (node-insert! node2 node9)
    (test-equal '((2 9 13)) (node->key-list 0 node13))
    (test-equal '((9 13) (2)) (node->key-list 1 node13))
    (test-equal '((13) (2) (9)) (node->key-list 2 node13))

    (node-insert! node13 node40)
    (test-equal '((2 9 13 40)) (node->key-list 0 node13))
    (test-equal '((9 13) (2 40)) (node->key-list 1 node13))
    (test-equal '((13) (2) (9) (40)) (node->key-list 2 node13))

    (node-insert! node40 node5)
    (test-equal '((2 5 9 13 40)) (node->key-list 0 node13))
    (test-equal '((5 9 13) (2 40)) (node->key-list 1 node13))
    (test-equal '((5 13) (2) (9) (40)) (node->key-list 2 node40))

  (let-values (([found path] (node-search node40 5)))
    (test-true found)
    (test-equal '((2 . 40) (1 . 40) (0 . 40) (0 . 13) (0 . 9)  (0 . 5) found) path)
    (test-equal "$5" (node-value found)))

  (let-values (([found path] (node-range-search node13 6 10)))
    (test-equal '((9 . "$9")) found))

  (node-delete! node13 9)
  (test-equal '((2 5 13 40)) (node->key-list 0 node13))
  (test-equal '((5 13) (2 40)) (node->key-list 1 node13))
  (test-equal '((5 13) (2) (40)) (node->key-list 2 node40))

  (node-delete! node5 2)
  (test-equal '((5 13 40)) (node->key-list 0 node13))
  (test-equal '((5 13) (40)) (node->key-list 1 node13))
  (test-equal '((5 13) (40)) (node->key-list 2 node40))

  (node-delete! node13 40)
  (test-equal '((5 13)) (node->key-list 0 node13))
  (test-equal '((5 13)) (node->key-list 1 node13))
  (test-equal '((5 13)) (node->key-list 2 node5))

))

(test-results)
