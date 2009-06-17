(library (skip graph)
  (export make-skip-graph skip-graph-add! skip-graph-level0
          make-node node-key node-value node-append! #;skip-graph-level0-key->list skip-graph-level1-key->list
          node-next node-prev node-membership node-search node-range-search node-search-closest<= node-add-level! search-same-membeship-node node-add!
          node->list node->key-list)
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

(define (node->key-list level start)
  (if (zero? level)
      (map node-key (node->list level start))
      (map (lambda (node*) (map node-key node*)) (node->list level start))))

(define (node->list level start)
  (define (collect-uniq-membership-node* start)
    (let ([node* (member-list 0 start)])
      (format #t "node*=~a\n" node*)
      (let loop ([node* node*]
                 [membership* '()]
                 [ret '()])
;      (display "[2]")
        (cond
         [(null? node*)
          (list-sort (lambda (a b) (< (node-key a) (node-key b))) ret)]
         [(memq (node-membership (car node*)) membership*)
          (loop (cdr node*) membership* ret)]
         [else
          (loop (cdr node*)
                (cons (node-membership (car node*)) membership*)
                (cons (car node*) ret))]))))
  (define (collect level node-direction start)
    (format #t "(collect ~d ~a ~a)\n" level node-direction start)
    (let loop ([node start]
               [ret '()])
      (cond
       [(not node)
        (reverse ret)]
       [else
        (loop (node-direction level node) (cons node ret))])))
  (define (member-list level start)
    (format #t "(member-list ~d ~a)\n" level start)
    (append (reverse (collect level node-prev start))
            (collect level node-next (node-next level start))))
  (cond
   [(zero? level)
    (member-list level start)]
   [else
    (let ([start-node* (collect-uniq-membership-node* start)])
      (map (lambda (node) (member-list level node)) start-node*))]
   ))


(define (search-same-membeship-node level start)
  (let loop ([left (node-prev level start)]
             [right (node-next level start)])
    (cond
     [(and (not left) (not right)) #f] ;; not found
     [(and left (= (node-membership left) (node-membership start)))
      left]
     [(and right (= (node-membership right) (node-membership start)))
      right]
     [else
      (loop (and left (node-prev level left))
            (and right (node-next level right)))])))

(define (node-add! start node)
  ;; level0
  (node-add-level! 0 start node)
  ;; level1
  (let ([buddy-node (search-same-membeship-node 0 node)])
    (when buddy-node
      (node-add-level! 1 buddy-node node))))

(define (node-add-level! level start node)
  (let ([closest-node (node-search-closest<= level start (node-key node))])
    (if (< (node-key node) (node-key closest-node)) ;; node to add is leftmost.
        (begin (display "hige") (node-append! level node closest-node))
        (node-insert! level closest-node node))))

(define (node-search-closest<= level start key)
  (node-search-with-level2 level start key))

(define (node-search-with-level2 level start key)
  (define (search-to-left)
    (let loop ([node start])
;      (format #t "level=~d node=~a\n" level (node-key node))
      (cond
       [(not (node-prev level node))
        node]
       [(<= (node-key (node-prev level node)) key)
        (node-prev level node)]
       [else
        (loop (node-prev level node))])))
  (define (search-to-right)
    (let loop ([node start])
;      (format #t "right level=~d node=~a\n" level (node-key node))
      (cond
       [(not (node-next level node))
        node]
       [(> (node-key (node-next level node)) key)
        node]
       [else
        (loop (node-next level node))])))
  (cond
   [(> (node-key start) key)
    ;; search to left
    (search-to-left)]
   [else
    ;; search to right
    (search-to-right)]))

(define (node-search-with-level level start key accum-path)
  (define (search-to-direction node-direction-proc key-cmp-proc)
    (let loop ([node start]
               [path (cons (cons level (node-key start)) accum-path)])
      ;;(format #t "level=~d node=~a\n" level (node-key node))
      (cond
       [(= (node-key node) key)
        (values #t node (cons 'found path))]
       [(not (node-direction-proc level node))
        (values #f node path)]
       [(key-cmp-proc (node-key (node-direction-proc level node)) key)
        (values #f node path)]
       [else
        (loop (node-direction-proc level node) (cons (cons level (node-key (node-direction-proc level node))) path))])))
  (cond
   [(> (node-key start) key)
    ;; search to left
    (search-to-direction node-prev <)]
   [else
    ;; search to right
    (search-to-direction node-next >)]))

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

(define (node-range-search start key1 key2)
  (assert (<= key1 key2))
  (let-values (([node path](node-search start key1)))
    (let loop ([node node]
               [ret '()])
      (cond
       [(>= key2 (node-key node))
        ;; always search on level0
        (loop (node-next 0 node) (cons node ret))]
       [else
        (values (reverse ret) path)]))))

(define (node->list-old level root)
  (let loop ([node root]
             [ret '()])
    (if node
        (loop (node-next level node) (cons node ret))
        (reverse ret))))

(define (node-key->list level root)
  (map node-key (node->list-old level root)))

;; (define (skip-graph-level0-key->list sg)
;;   (node-key->list 0 (skip-graph-level0 sg)))

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
         (skip-graph-level10-set! sg node))]
   [else
    (aif (skip-graph-level11 sg)
         (skip-graph-level11-set! sg (node-insert! 1 it node))
         (skip-graph-level11-set! sg node))]))

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
   (mutable next*))
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
