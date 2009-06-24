(library (skip graph)
  (export make-node node-key node-value membership=?
          node-right node-left node-membership node-search node-range-search node-insert!
          node->list node->key-list max-level membership-counter
          node-delete!
          ;; export for test
          membership-level
          buddy-op
          link-op)
  (import (rnrs)
          (mosh)
          (only (srfi :1) drop)
          (srfi :42)
          (srfi :39)
          (mosh control))

;; Dynamic parameters
(define max-level (make-parameter 1))
(define membership-counter (make-parameter 0))

;; public skip graph manipulations
(define (node-search start key)
  (let-values (([node path] (search-op start start key (max-level) '())))
    (if (= key (node-key node))
        (values node path)
        (values #f path))))

(define (node-range-search start key1 key2 . opt)
  (let-optionals* opt ((limit 0))
    (assert (<= key1 key2))
    (let-values (([node path] (search-op start start key1 (max-level) '())))
      (values (reverse (range-search-op node start key2 '() limit)) path))))

(define (node-insert! introducer n)
  (insert-op introducer n))

(define (node-delete! introducer key)
  (let loop ([level (max-level)])
    (cond
     [(< level 0) '()]
     [else
       (let ([node (search-op introducer introducer key level '())])
         (unless (= (node-key node) key)
           (error 'node-delete! "key does not exist"))
          (aif (node-left level node)
            (delete-op it (node-right level node) level 'RIGHT))
          (aif (node-right level node)
            (delete-op it (node-left level node) level 'LEFT)))
      (loop (- level 1))])))

;; Inspection for Debug
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

;; delete operation
(define (delete-op self side-node level side)
  ((if (eq? side 'LEFT) node-left-set! node-right-set!) level self side-node))

;; search operation
(define (search-op self start key level path)
  (define (search-op-result start found-node path)
    (values found-node (reverse path)))
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

;; range-search operation
(define (range-search-op self start key-max accum-key/value* limit)
  (define (return-range-search-op start results)
    results)
  (cond
   [(> (node-key self) key-max)
    (return-range-search-op start accum-key/value*)]
   [(< (node-key self) key-max)
    (aif (node-right 0 self)
         (if (or (not (zero? limit)) (= (- limit 1) (length accum-key/value*)))
             (return-range-search-op start (cons (cons (node-key self) (node-value self))
                                                 accum-key/value*))
             (range-search-op it start key-max (cons (cons (node-key self) (node-value self))
                                                     accum-key/value*) limit)))]
   [else ; (= (node-key self) key-max)
    (return-range-search-op start (cons (cons (node-key self) (node-value self))
                                        accum-key/value*))]))

;; link operation
(define (link-op self n side level)
  (assert (and self n))
  (case side
    [(RIGHT)
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
           (link-op right n 'LEFT level))])
       (node-left-set! level n self)
       (node-right-set! level n right))]
    [(LEFT)
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
           (link-op left n 'RIGHT level))])
       (node-left-set! level n left)
       (node-right-set! level n self))]
    [else
     (assert #f)]))

;; buddy operation
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

;; insert operation
(define (insert-op introducer n)
  (cond
   [(eq? introducer n)
    (node-right-set! 0 n #f)
    (node-left-set!  0 n #f)]
   [else
    (let-values (([neighbor path] (search-op introducer n (node-key n) 0 '())))
      (link-op neighbor n (if (< (node-key introducer) (node-key n)) 'RIGHT 'LEFT) 0)
      (let loop ([level 1])
        (cond
         [(> level (max-level)) '()]
         [(node-left (- level 1) n)
          (let ([new-buddy (buddy-op (node-left (- level 1) n) n level (membership-level level (node-membership n)) 'RIGHT)])
            (cond
             [new-buddy
              (link-op new-buddy n 'RIGHT level)
              (loop (+ level 1))]
             [(node-right (- level 1) n)
              (let ([new-buddy (buddy-op (node-right (- level 1) n) n level (membership-level level (node-membership n)) 'LEFT)])
                (cond
                 [new-buddy
                  (link-op new-buddy n 'LEFT level)
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
              (link-op new-buddy n 'LEFT level)
              (loop (+ level 1))]
             [(node-left (- level 1) n)
              (let ([new-buddy (buddy-op (node-left (- level 1) n) n level (membership-level level (node-membership n)) 'RIGHT)])
                (cond
                 [new-buddy
                  (link-op new-buddy n 'RIGHT level)
                  (loop (+ level 1))]
                 [else
                  '() ;; nomore
                  ]))]
             [else
              '()])
            )]
         [else
          '()])))]))

;; Membership vector
;; For testability, this issues sequencial number.
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
  (equal? (membership-level level (node-membership n1))
          (membership-level level (node-membership n2))))

;; node manipulation
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
)
