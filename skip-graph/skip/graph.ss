(library (skip graph)
  (export make-node node-key node-value node-append! membership=?
          node-right node-left node-membership node-search node-range-search node-search-closest<= search-same-membeship-node node-insert!
          node->list node->key-list max-level membership-counter
          node-delete!
          ;; export for test
          membership-level
          buddy-op
          link-op
          )
  (import (rnrs)
          (mosh)
          (only (srfi :1) drop)
          (srfi :39)
          (srfi :42)
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

(define (link-op self n side level)
  (assert (and self n))
  (case side
    [(R)
     (let ([left self]
           [right (node-right level self)])
       (cond
        [(and right (< (node-key right) (node-key self)))
         ;; Someone inserted the other node as follows.
         ;;   Before 30 => 50
         ;;             40
         ;;   Now 30 => 33 => 50
         ;;                40
         ;; We resend link-op to the right
         (assert #f) ;; not tested
         (link-op right n side level)]
        [else
         (node-right-set! level self n)
         ;; tell the neighbor to link the newone
         (when right
           (link-op right n 'L level))])
       (node-left-set! level n self)
       (node-right-set! level n right))]
    [(L)
     (let ([right self]
           [left (node-left level self)])
       (cond
        [(and left (> (node-key left) (node-key self)))
         ;; Someone inserted the other node as follows.
         ;;   Before 30 => 40
         ;;             35
         ;;   Now    30 => 37 => 40
         ;;             35
         ;; We resend link-op to the left
         (assert #f) ;; not tested
         (link-op left n side level)]
        [else
         (node-left-set! level self n)
         ;; tell the neighbor to link the newone
         (when left
           (link-op left n 'R level))])
       (node-left-set! level n left)
       (node-right-set! level n self))]
    [else
     (assert #f)]))

;; (define (link-op self n side level)
;;   (case side
;;     [(R)
;;      (let ([left self]
;;            [right (node-right level self)])
;;        (when left
;;          (node-right-set! level left n))
;;        (when right
;;          (node-left-set! level right n))
;;        (node-left-set! level n left)
;;        (node-right-set! level n right))]
;;     [(L)
;;      (let ([left (node-left level self)]
;;            [right self])
;;        (when left
;;          (node-right-set! level left n))
;;        (when right
;;          (node-left-set! level right n))
;;        (node-left-set! level n left)
;;        (node-right-set! level n right))]
;;     [else
;;      (assert #f)]))

(define (buddy-op self n level membership side)
  (cond
   [(membership=? level n self)
    self]
   [else
    (case side
      [(LEFT)
       (if (node-right (- level 1) self)
           (buddy-op (node-right (- level 1) self) n level membership side)
           #f)]
      [(RIGHT)
       (if (node-left (- level 1) self)
           (buddy-op (node-left (- level 1) self) n level membership side)
           #f)]
      [else
       (assert #f)])]))

(define (node-insert! introducer n)
  (cond
   [(eq? introducer n)
    (node-right-set! 0 n #f)
    (node-left-set!  0 n #f)]
   [else
    (let-values (([neighbor path] (search-op introducer n (node-key n) 0 '())))
      (link-op neighbor n (if (< (node-key introducer) (node-key n)) 'R 'L) 0)
      (let loop ([level 1])
        (cond
         [(> level (max-level)) '()]
         [(node-left (- level 1) n)
          (let ([new-buddy (buddy-op (node-left (- level 1) n) n level (membership-level level (node-membership n)) 'RIGHT)])
            (cond
             [new-buddy
              (link-op new-buddy n 'R level)
              (loop (+ level 1))]
             [(node-right (- level 1) n)
              (let ([new-buddy (buddy-op (node-right (- level 1) n) n level (membership-level level (node-membership n)) 'LEFT)])
                (cond
                 [new-buddy
                  (link-op new-buddy n 'L level)
                  (loop (+ level 1))]
                 [else
                  '() ;; nomore
                  ]))]
             [else
              '()]))]
         [(node-right (- level 1) n)
          (let ([new-buddy (buddy-op (node-right (- level 1) n) n level (membership-level level (node-membership n)) 'LEFT)])
            (cond
             [new-buddy
              (link-op new-buddy n 'L level)
              (loop (+ level 1))]
             [(node-left (- level 1) n)
              (let ([new-buddy (buddy-op (node-left (- level 1) n) n level (membership-level level (node-membership n)) 'RIGHT)])
                (cond
                 [new-buddy
                  (link-op new-buddy n 'R level)
                  (loop (+ level 1))]
                 [else
                  '() ;; nomore
                  ]))]
             [else
              '()])
            )]
         [else
          '()])))]))

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
  (let-values (([node path] (search-op start start key (max-level) '())))
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

(define (node-range-search start key1 key2 . opt)
  (let-optionals* opt ((limit #f))
    (assert (<= key1 key2))
    (let-values (([node path] (node-search-internal start key1)))
      (let loop ([node (if (= key1 (node-key node))
                           node
                           (node-right 0 node))]
                 [ret '()]
                 [count 0])
        (cond
         [(and limit (= limit count))
          (values (reverse ret) path)]
         [(>= key2 (node-key node))
          ;; always search on level0
          (loop (node-right 0 node) (cons node ret) (+ 1 count))]
         [else
          (values (reverse ret) path)])))))

;; For testability, this is sequencial
(define (gen-membership)
  ;; (num->binary-list 3 0) => (0 0 0)
  ;; (num->binary-list 3 1) => (0 0 1)
  ;; (num->binary-list 3 2) => (0 1 0)
  (define (num->binary-list bits n)
    (let ([ret (map (lambda (x) (- x (char->integer #\0))) (map char->integer (string->list (number->string n 2))))])
      (if (> bits (length ret))
          (append (list-ec (: i (- bits (length ret))) 0) ret)
          ret)))
  (let ([ret (num->binary-list (max-level) (membership-counter))])
    (membership-counter (+ (membership-counter) 1))
    (when (= (expt 2 (max-level)) (membership-counter))
      (membership-counter 0))
    ret))

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

;; send result node to start node.
(define (search-op-result start found-node path)
  (values found-node (reverse path)))

(define (search-op self start key level path)
  (define (add-path level)
    (cons (cons level (node-key self)) path))
  (cond
   [(= (node-key self) key)
    (search-op-result start self (cons 'found (add-path level)))]
   [(< (node-key self) key)
    (let loop ([level level])
      (cond
       [(< level 0)
        (search-op-result start self path)]
       [(and (node-right level self) (<= (node-key (node-right level self)) key))
        (search-op (node-right level self) start key level (add-path level))]
       [else
        (set! path (add-path level))
        (loop (- level 1))]))]
   [else
    (let loop ([level level])
      (cond
       [(< level 0)
        (search-op-result start self path)]
       [(and (node-left level self) (>= (node-key (node-left level self)) key))
        (search-op (node-left level self) start key level (add-path level))]
       [else
        (set! path (add-path level))
        (loop (- level 1))]))]))

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

(define (node-delete! node)
  (let loop ([level (max-level)])
    (cond
     [(< level 0) '()]
     [else
      (when (node-left level node)
        (node-right-set! level (node-left level node) (node-right level node)))
      (when (node-right level node)
        (node-left-set! level (node-right level node) (node-left level node)))
      (loop (- level 1))])))

;; (define (node-insert! level root node)
;;   (define (node< a b)
;;     (< (node-key a) (node-key b)))
;;   (cond
;;    [(node< node root)
;;     (node-append! level node root)
;;     ;; root is changed
;;     node]
;;    [else
;;     (let loop ([n root])
;;       (cond
;;        [(not (node-right level n)) ;; tail
;;         (node-append! level n node)
;;         root]
;;        [(node< node (node-right level n))
;;         (let ([right (node-right level n)])
;;           (node-append! level n node)
;;           (node-right-set! level node right)
;;           (node-left-set! level right node)
;;           root)]
;;        [else
;;         (loop (node-right level n))]))]))
)
