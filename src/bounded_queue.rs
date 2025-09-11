use std::collections::VecDeque;

pub trait VecDequeExt<T> {
    /// Push new values keeping the capacity. If capacity is reached, evict the oldest item.
    fn bounded_push_back(&mut self, item: T);
}

impl<T> VecDequeExt<T> for VecDeque<T> {
    fn bounded_push_back(&mut self, item: T) {
        if self.len() >= self.capacity() {
            self.pop_front(); // Evict oldest
        }
        self.push_back(item);
    }
}

#[cfg(test)]
mod test_bounded_queue {
    use super::VecDequeExt;
    use std::collections::VecDeque;

    #[test]
    fn test_bounded_push() {
        let mut queue = VecDeque::with_capacity(2);
        queue.bounded_push_back(1);
        queue.bounded_push_back(2);
        queue.bounded_push_back(3); // should evict 1
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.capacity(), 2);
        assert_eq!(queue.front(), Some(&2));
        assert_eq!(queue.back(), Some(&3));
        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.pop_front(), Some(3));
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.capacity(), 2);
    }
}
