(library (skip graph)
  (export make-node node-key node-value node-append! membership=?
          node-right node-left node-membership node-search node-range-search node-search-closest<= node-add-level! search-same-membeship-node node-add!
          node->list node->key-list max-level membership-counter
          ;; export for test
          membership-level
          )
  (import (rnrs)
          (mosh)
          (only (srfi :1) drop)
          (srfi :39)
          (mosh control))

(define max-level (make-parameter 1))
(define membership-counter (make-parameter 0))

(define (node->key-list level start)
  (map (lambda (node*) (map node-key node*)) (node->list level start)))

(define (node->list level start)
  (define (collect-uniq-membership-node* start)
    (let ([node* (member-list 0 start)])
      (let loop ([node* node*]
                 [membership* '()]
                 [ret '()])
        (cond
         [(null? node*)
          (list-sort (lambda (a b) (membership< level a b)) ret)]
         [(member (membership-level level (node-membership (car node*))) membership*)
          (loop (cdr node*) membership* ret)]
         [else
          (loop (cdr node*)
                (cons (membership-level level (node-membership (car node*))) membership*)
                (cons (car node*) ret))]))))
  (define (collect level node-direction start)
    (let loop ([node start]
               [ret '()])
      (cond
       [(not node)
        (reverse ret)]
       [else
        (loop (node-direction level node) (cons node ret))])))
  (define (member-list level start)
    (append (reverse (collect level node-left start))
            (collect level node-right (node-right level start))))
  (cond
   [(zero? level)
    (list (member-list level start))]
   [else
    (let ([start-node* (collect-uniq-membership-node* start)])
      (map (lambda (node) (member-list level node)) start-node*))]))

(define (search-same-membeship-node search-level membership-level start)
  ;; always search in level0
  (let loop ([left (node-left search-level start)]
             [right (node-right search-level start)])
    (cond
     [(and (not left) (not right)) #f] ;; not found
     [(and left (membership=? membership-level left start))
      left]
     [(and right (membership=? membership-level right start))
      right]
     [else
      (loop (and left (node-left search-level left))
            (and right (node-right search-level right)))])))

(define (node-add! start node)
;  (format #t "node-to-add key=~a membership=~a\n" (node-key node) (node-membership node))
  ;; level0
  (node-add-level! 0 start node)
  ;; level1 or higher
  (let loop ([level 1])
    (cond
     [(> level (max-level)) '()]
     [else
      (let ([buddy-node (search-same-membeship-node (- level 1) level node)])
        (when buddy-node
          (node-add-level! level buddy-node node)))
      (loop (+ level 1))])))

(define (node-add-level! level start node)
  (let ([closest-node (node-search-closest<= level start (node-key node))])
    (if (< (node-key node) (node-key closest-node)) ;; node to add is leftmost.
        (node-append! level node closest-node)
        (node-insert! level closest-node node))))

(define (node-search-closest<= level start key)
  (define (search-to-left)
    (let loop ([node start]
               [path (list (cons level (node-key start)))])
      (cond
       [(not (node-left level node))
        (values node (reverse path))]
       [(<= (node-key (node-left level node)) key)
        (values (node-left level node) (reverse (cons (cons level (node-key (node-left level node))) path)))]
       [else
        (loop (node-left level node) (cons (cons level (node-key (node-left level node))) path))])))
  (define (search-to-right)
    (let loop ([node start]
               [path (list (cons level (node-key start)))])
      (cond
       [(not (node-right level node))
        (values node (reverse path))]
       [(> (node-key (node-right level node)) key)
        (values node (reverse path))]
       [else
        (loop (node-right level node) (cons (cons level (node-key (node-right level node))) path))])))
  (cond
   [(> (node-key start) key)
    ;; search to left
    (search-to-left)]
   [else
    ;; search to right
    (search-to-right)]))

(define (node-search start key)
  (let-values (([node path] (node-search-internal start key)))
    (if (= key (node-key node))
        (values node path)
        (values #f path))))

(define (node-search-internal start key)
  (let loop ([level (max-level)] ;; start search on max-level
             [start start]
             [path '()])
    (cond
     [(< level 0) (values start path)]
     [else
      (let-values (([found-node accum-path] (node-search-closest<= level start key)))
        (if (= (node-key found-node) key)
            (values found-node (append path accum-path '(found)))
            (loop (- level 1) found-node (append path accum-path))))])))

(define (node-range-search start key1 key2)
  (assert (<= key1 key2))
  (let-values (([node path] (node-search-internal start key1)))
    (let loop ([node (if (= key1 (node-key node))
                         node
                         (node-right 0 node))]
               [ret '()])
      (cond
       [(>= key2 (node-key node))
        ;; always search on level0
        (loop (node-right 0 node) (cons node ret))]
       [else
        (values (reverse ret) path)]))))

(define (gen-membership)
  (cond
   [(= (max-level) 1)
    (cond
     [(zero? (membership-counter))
      (membership-counter 1)
      '(0)]
     [else
      (membership-counter 0)
      '(1)])]
   [(= (max-level) 2)
    ;; ((number->string x 2)
    (case (membership-counter)
      [(0) '(0 0)]
      [(1) '(0 1)]
      [(2) '(1 0)]
      [(3) '(1 1)])])
    (membership-counter (+ (membership-counter) 1))
    (when (= 4 (membership-counter))
      (membership-counter 0))
    ret)]
   [else
    #f
    ]))

(define (membership-level level membership)
  (drop membership (- (length membership) level)))

(define (membership< level n1 n2)
  (define (to-number node)
    (string->number (apply string-append (map number->string (membership-level level (node-membership node)))) 2))
  (< (to-number n1) (to-number n2)))
(define (membership=? level n1 n2)
;  (format #t "level=~a ~a(~a), ~a(~a)\n" level (node-key n1) (membership-level level (node-membership n1)) (node-key n2) (membership-level level (node-membership n2)))
  (equal? (membership-level level (node-membership n1))
          (membership-level level (node-membership n2))))

(define-record-type node
  (fields
   (immutable key)
   (immutable value)
   (immutable membership)
   (mutable left*)
   (mutable right*))
  (protocol
   (lambda (c)
     (lambda (key value)
       (c key value (gen-membership) (make-vector (+ (max-level) 1) #f) (make-vector (+ (max-level) 1) #f))))))

(define (node-right level n)
  (vector-ref (node-right* n) level))

(define (node-left level n)
  (vector-ref (node-left* n) level))

(define (node-right-set! level n1 n2)
  (vector-set! (node-right* n1) level n2))

(define (node-left-set! level n1 n2)
  (vector-set! (node-left* n1) level n2))

(define (node-append! level n1 n2)
  (node-right-set! level n1 n2)
  (node-left-set! level n2 n1))

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
       [(not (node-right level n)) ;; tail
        (node-append! level n node)
        root]
       [(node< node (node-right level n))
        (let ([right (node-right level n)])
          (node-append! level n node)
          (node-right-set! level node right)
          (node-left-set! level right node)
          root)]
       [else
        (loop (node-right level n))]))]))
)
