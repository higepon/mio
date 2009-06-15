(library (skip graph)
  (export make-skip-graph skip-graph-add! skip-graph-level0
          make-node node-key node-value node-append0! skip-graph-level0-key->list skip-graph-level1-key->list
          node-next0 node-prev0 node-membership
          )
  (import (rnrs)
          (mosh control))

(define-record-type skip-graph
  (fields
   (mutable level0)
   (mutable level10)
   (mutable level11))
  (protocol
   (lambda (c)
     (lambda ()
       (c #f #f #f)))))

(define (node0->list root)
  (let loop ([node root]
             [ret '()])
    (if node
        (loop (node-next0 node) (cons node ret))
        (reverse ret))))

(define (node1->list root)
  (let loop ([node root]
             [ret '()])
    (if node
        (loop (node-next1 node) (cons node ret))
        (reverse ret))))

(define (node0-key->list root)
  (map node-key (node0->list root)))

(define (node1-key->list root)
  (map node-key (node1->list root)))


(define (skip-graph-level0-key->list sg)
  (node0-key->list (skip-graph-level0 sg)))

(define (skip-graph-level1-key->list sg)
  (list
   (node1-key->list (skip-graph-level10 sg))
   (node1-key->list (skip-graph-level11 sg))))

(define (skip-graph-add! sg node)
  (aif (skip-graph-level0 sg)
       (skip-graph-level0-set! sg (node-insert0! it node))
       (skip-graph-level0-set! sg node))
  (cond
   [(zero? (node-membership node))
    (aif (skip-graph-level10 sg)
         (skip-graph-level10-set! sg (node-insert10! it node))
         (skip-graph-level10-set! sg node))
    ]
   [else
    (aif (skip-graph-level11 sg)
         (skip-graph-level11-set! sg (node-insert11! it node))
         (skip-graph-level11-set! sg node))
    ]))

(define membership 0)

(define (gen-membership)
  (cond
   [(zero? membership)
    (set! membership 1)
    0]
   [else
    (set! membership 0)
    1]))

(define-record-type node
  (fields
   (immutable key)
   (immutable value)
   (immutable membership)
   (mutable prev0)
   (mutable prev1)
   (mutable next0)
   (mutable next1))
  (protocol
   (lambda (c)
     (lambda (key value)
       (c key value (gen-membership) #f #f #f #f)))))

(define (node-append0! n1 n2)
  (node-next0-set! n1 n2)
  (node-prev0-set! n2 n1))

(define (node-append1! n1 n2)
  (node-next1-set! n1 n2)
  (node-prev1-set! n2 n1))

(define (node-insert0! root node)
  (define (node< a b)
    (< (node-key a) (node-key b)))
  (cond
   [(node< node root)
    (node-append0! node root)
    ;; root is changed
    node]
   [else
    (let loop ([n root])
      (cond
       [(not (node-next0 n)) ;; tail
        (node-append0! n node)
        root]
       [(node< node (node-next0 n))
        (let ([next (node-next0 n)])
          (node-append0! n node)
          (node-next0-set! node next)
          (node-prev0-set! next node)
          root)]
       [else
        (loop (node-next0 n))]))]))

(define (node-insert10! root node)
  (define (node< a b)
    (< (node-key a) (node-key b)))
  (cond
   [(node< node root)
    (node-append1! node root)
    ;; root is changed
    node]
   [else
    (let loop ([n root])
      (cond
       [(not (node-next1 n)) ;; tail
        (node-append1! n node)
        root]
       [(node< node (node-next1 n))
        (let ([next (node-next1 n)])
          (node-append1! n node)
          (node-next1-set! node next)
          (node-prev1-set! next node)
          root)]
       [else
        (loop (node-next1 n))]))]))
(define node-insert11! node-insert10!)





)
