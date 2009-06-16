(library (skip graph)
  (export make-skip-graph skip-graph-add! skip-graph-level0
          make-node node-key node-value node-append! skip-graph-level0-key->list skip-graph-level1-key->list
          node-next node-prev node-membership node-search
          )
  (import (rnrs)
          (mosh)
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

(define (node-search-with-level level start key accum-path)
  (cond
   [(= (node-key start) key)
    (values #t start (cons 'found accum-path))]
   [(> (node-key start) key)
    ;; search to left
    (let loop ([node start]
               [path (cons (cons level (node-key start)) accum-path)])
;;      (format #t "level=~d node=~a\n" level (node-key node))
      (cond
       [(= (node-key node) key)
        (values #t node (cons 'found path))]
       [(not (node-prev level node))
        (values #f node path)]
       [(< (node-key (node-prev level node)) key)
        (values #f node path)]
       [else
        (loop (node-prev level node) (cons (cons level (node-key (node-prev level node))) path))]))]
   [else
    ;; search to right
    (let loop ([node start]
               [path (cons (cons level (node-key start)) accum-path)])
;;      (format #t "level=~d node=~a\n" level (node-key node))
      (cond
       [(= (node-key node) key)
        (values #t node (cons 'found path))]
       [(not (node-next level node))
        (values #f node path)]
       [(> (node-key (node-next level node)) key)
        (values #f node path)]
       [else
        (loop (node-next level node) (cons (cons level (node-key (node-next level node))) path))]))]))

(define (node-search start key)
  (let loop ([level 1] ;; start search on level 1
             [start start]
             [path '()])
    (cond
     [(< level 0) (values #f (reverse path))]
     [else
      (let-values (([found? found-node accum-path] (node-search-with-level level start key path)))
        (if found?
            (values found-node (reverse accum-path))
            (loop (- level 1) found-node accum-path)))])))


(define (node->list level root)
  (let loop ([node root]
             [ret '()])
    (if node
        (loop (node-next level node) (cons node ret))
        (reverse ret))))

(define (node-key->list level root)
  (map node-key (node->list level root)))

(define (skip-graph-level0-key->list sg)
  (node-key->list 0 (skip-graph-level0 sg)))

(define (skip-graph-level1-key->list sg)
  (list
   (node-key->list 1 (skip-graph-level10 sg))
   (node-key->list 1 (skip-graph-level11 sg))))

;; todo refactoring
(define (skip-graph-add! sg node)
  (aif (skip-graph-level0 sg)
       (skip-graph-level0-set! sg (node-insert! 0 it node))
       (skip-graph-level0-set! sg node))
  (cond
   [(zero? (node-membership node))
    (aif (skip-graph-level10 sg)
         (skip-graph-level10-set! sg (node-insert! 1 it node))
         (skip-graph-level10-set! sg node))
    ]
   [else
    (aif (skip-graph-level11 sg)
         (skip-graph-level11-set! sg (node-insert! 1 it node))
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
   (mutable prev*)
   (mutable next*)
   )
  (protocol
   (lambda (c)
     (lambda (key value)
       (c key value (gen-membership) (make-vector 2 #f) (make-vector 2 #f))))))

(define (node-next level n)
  (vector-ref (node-next* n) level))

(define (node-prev level n)
  (vector-ref (node-prev* n) level))

(define (node-next-set! level n1 n2)
  (vector-set! (node-next* n1) level n2))

(define (node-prev-set! level n1 n2)
  (vector-set! (node-prev* n1) level n2))

(define (node-append! level n1 n2)
  (node-next-set! level n1 n2)
  (node-prev-set! level n2 n1))

(define (node-insert! level root node)
  (define (node< a b)
    (< (node-key a) (node-key b)))
  (cond
   [(node< node root)
    (node-append! level node root)
    ;; root is changed
    node]
   [else
    (let loop ([n root])
      (cond
       [(not (node-next level n)) ;; tail
        (node-append! level n node)
        root]
       [(node< node (node-next level n))
        (let ([next (node-next level n)])
          (node-append! level n node)
          (node-next-set! level node next)
          (node-prev-set! level next node)
          root)]
       [else
        (loop (node-next level n))]))]))





)
